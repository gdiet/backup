package dedup

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.StandardOpenOption.{CREATE_NEW, READ, SPARSE, WRITE}
import java.nio.file.{Files, Path}

import org.slf4j.{Logger, LoggerFactory}

class CacheEntries(tempPath: String, readOnly: Boolean) extends AutoCloseable {
  private val tempDir: File = new File(tempPath, "dedupfs-temp")
  if (!readOnly) {
    require(tempDir.isDirectory || tempDir.mkdirs(), s"Can't create temp dir $tempDir")
    require(tempDir.list().isEmpty, s"Temp dir is not empty: $tempDir")
  }
  require(tempDir.isDirectory && tempDir.list().isEmpty || tempDir.mkdirs())

  private val log: Logger = LoggerFactory.getLogger(getClass)
  private val memoryCacheSize: Long = Server.freeMemory*3/4 - 128000000
  log.info(s"Initializing data store with memory cache size ${memoryCacheSize / 1000000}MB")
  require(memoryCacheSize > 8000000, "Not enough free memory for a sensible memory cache.")

  // Map(id/dataId -> size, Seq(data))
  private var cacheEntries = Map[(Long, Long), (Long, Seq[Entry])]()
  private var memoryUsage = 0L
  private var openChannels: Map[(Long, Long), SeekableByteChannel] = Map()

  private def path(id: Long, dataId: Long): Path = new File(tempDir, s"$id-$dataId").toPath
  private def channel(id: Long, dataId: Long): SeekableByteChannel = {
    openChannels.getOrElse(id -> dataId, {
      log.info(s"Memory cache full, creating temporary store file ($id/$dataId)")
      Files.newByteChannel(path(id, dataId), WRITE, CREATE_NEW, SPARSE, READ)
        .tap(channel => openChannels += ((id, dataId) -> channel))
    })
  }

  def newEntry(id: Long, dataId: Long, position: Long, data: Array[Byte]): Entry =
    if (memoryUsage + data.length > memoryCacheSize)
      FileEntry(id, dataId, position, data.length).tap(_.write(data))
    else MemoryEntry(id, dataId, position, data)

  def getEntry(id: Long, dataId: Long): Option[(Long, Seq[Entry])] = cacheEntries.get(id -> dataId)

  def setOrReplace(id: Long, dataId: Long, size: Long, entries: Seq[Entry]): Unit = {
    clearEntry(id, dataId)
    memoryUsage += entries.map(_.memory).sum
    cacheEntries += (id, dataId) -> (size, entries)
  }

  def clearEntry(id: Long, dataId: Long): Unit = {
    log.debug(s"Clear entry $id/$dataId - memory usage before is $memoryUsage.")
    memoryUsage -= cacheEntries.get(id -> dataId).toSeq.flatMap(_._2).map(_.memory).sum
    cacheEntries -= (id -> dataId)
  }

  def delete(id: Long, dataId: Long): Unit = {
    openChannels.get(id -> dataId).foreach { c =>
      c.close()
      log.debug(s"Deleting temporary store file for $id/$dataId")
      Files.delete(path(id, dataId))
    }
    openChannels -= id -> dataId
    clearEntry(id, dataId)
  }

  override def close(): Unit = {
    openChannels.keys.foreach { case (id, dataId) => delete(id, dataId) }
  }

  sealed trait Entry {
    def id: Long; def dataId: Long
    def position: Long; def data: Array[Byte]
    def length: Int; def memory: Long
    def write(data: Array[Byte]): Unit
    def drop(left: Int, right: Int): Entry
    def ++(other: Entry): Entry
  }

  private case class MemoryEntry(id: Long, dataId: Long, position: Long, data: Array[Byte]) extends Entry {
    override def toString: String = s"Mem($id/$dataId, $position, $length)"
    override def length: Int = data.length
    override def memory: Long = length + 500
    override def drop(left: Int, right: Int): Entry = copy(position = position + left, data = data.drop(left).dropRight(right))
    override def write(data: Array[Byte]): Unit = System.arraycopy(data, 0, this.data, 0, data.length)
    override def ++(other: Entry): Entry = newEntry(id, dataId, position, data ++ other.data)
  }

  private case class FileEntry(id: Long, dataId: Long, position: Long, length: Int) extends Entry {
    override def toString: String = s"Fil($id/$dataId, $position, $length)"
    override def data: Array[Byte] = {
      val buffer = ByteBuffer.allocate(length)
      val input = channel(id, dataId).position(position)
      while(buffer.remaining() > 0) input.read(buffer)
      new Array[Byte](length).tap(buffer.position(0).get)
    }
    override def memory: Long = 500
    override def drop(left: Int, right: Int): Entry = {
      copy(position = position + left, length = length - left - right)
    }
    override def write(data: Array[Byte]): Unit = {
      channel(id, dataId).position(position).write(ByteBuffer.wrap(data))
    }
    override def ++(other: Entry): Entry = {
      require(other.id == id && other.dataId == dataId)
      require(position + length == other.position)
      other match {
        case _: FileEntry => /* Nothing to do. */
        case _: MemoryEntry => channel(id, dataId).position(position + length).write(ByteBuffer.wrap(other.data))
      }
      copy(length = length + other.length)
    }
  }
}
