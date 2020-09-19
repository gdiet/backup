package dedup

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.StandardOpenOption.{CREATE_NEW, READ, SPARSE, WRITE}
import java.nio.file.{Files, Path}

import org.slf4j.{Logger, LoggerFactory}

// TODO In read-only mode there is no need to initialize cache entries.
class CacheEntriesOld(tempPath: String, readOnly: Boolean) extends AutoCloseable {
  implicit private val log: Logger = LoggerFactory.getLogger(getClass)
  private val tempDir: File = new File(tempPath, "dedupfs-temp")
  if (!readOnly) {
    require(tempDir.isDirectory || tempDir.mkdirs(), s"Can't create temp dir $tempDir")
    require(tempDir.list().isEmpty, s"Temp dir is not empty: $tempDir")
  }

  private val memoryCacheSize: Long = 128000000 // Server.freeMemory*3/4 - 128000000
  log.debug(s"Initializing data store with memory cache size ${memoryCacheSize / 1000000}MB")
  require(memoryCacheSize > 8000000, "Not enough free memory for a sensible memory cache.")

  // Map(id/dataId -> size, Seq(data))
  private var cacheEntries = Map[(Long, Long), (Long, Seq[Entry])]()
  private var memoryUsage = 0L
  private var openChannels: Map[(Long, Long), SeekableByteChannel] = Map()

  override def toString: String = s"Cache(\n${cacheEntries.mkString("\n")}\n)"

  private def path(id: Long, dataId: Long): Path = new File(tempDir, s"$id-$dataId").toPath
  private def channel(id: Long, dataId: Long): SeekableByteChannel = {
    openChannels.getOrElse(id -> dataId, {
      log.debug(s"Memory cache full, creating temporary store file ($id/$dataId)")
      Files.newByteChannel(path(id, dataId), WRITE, CREATE_NEW, SPARSE, READ)
        .tap(channel => openChannels += ((id, dataId) -> channel))
    })
  }

  def newEntry(id: Long, dataId: Long, position: Long, data: Array[Byte], additionalMemory: Long = 0): Entry =
    if (memoryUsage + data.length + additionalMemory > memoryCacheSize)
      FileEntry(id, dataId, position, data.length).tap(_.write(0, data))
    else MemoryEntry(id, dataId, position, data)

  def getEntry(id: Long, dataId: Long): Option[(Long, Seq[Entry])] = cacheEntries.get(id -> dataId)

  def setOrReplace(id: Long, dataId: Long, size: Long, entries: Seq[Entry]): Unit = {
    clearEntry(id, dataId)
    memoryUsage += entries.map(_.memory).sum
    cacheEntries += (id, dataId) -> (size, entries)
  }

  def clearEntry(id: Long, dataId: Long): Unit = {
    log.trace(s"Clear entry $id/$dataId - memory usage before is ${memoryUsage/1000000}MB.")
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
    final def write(offset: Int, data: Array[Byte]): Unit = {
      assumeLogged(offset >= 0, s"offset >= 0 ... $offset")
      assumeLogged(offset + data.length <= length, s"offset + data.length <= length ... $offset + ${data.length} < $length")
      writeImpl(offset, data)
    }
    protected def writeImpl(offset: Int, data: Array[Byte]): Unit
    final def drop(left: Int, right: Int): Entry = {
      assumeLogged(left >= 0, s"left >= 0 ... $left")
      assumeLogged(right >= 0, s"right >= 0 ... $right")
      assumeLogged(left + right <= length, s"left + right <= length ... $left / $right / $length")
      dropImpl(left, right)
    }
    protected def dropImpl(left: Int, right: Int): Entry
  }

  private case class MemoryEntry(id: Long, dataId: Long, position: Long, data: Array[Byte]) extends Entry {
    override def toString: String = s"Mem($id/$dataId, $position, $length)"
    override def length: Int = data.length
    override def memory: Long = length + 500
    override def dropImpl(left: Int, right: Int): Entry = copy(position = position + left, data = data.drop(left).dropRight(right))
    override def writeImpl(offset: Int, data: Array[Byte]): Unit = System.arraycopy(data, 0, this.data, offset, data.length)
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
    override def dropImpl(left: Int, right: Int): Entry =
      copy(position = position + left, length = length - left - right)
    override def writeImpl(offset: Int, data: Array[Byte]): Unit =
      channel(id, dataId).position(position + offset).write(ByteBuffer.wrap(data))
  }
}
