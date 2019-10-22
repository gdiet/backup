package dedup

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.StandardOpenOption._
import java.nio.file.{Files, Path}

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.SortedMap
import scala.io.StdIn

object DataStoreResearch extends App {
  val blocks = 200
  def delete(file: File): Unit = {
    if (file.isDirectory) file.listFiles.foreach(delete)
    file.delete()
  }
  delete(new File("data"))
  val ds = new DataStore("data", false)
  System.gc()
  Thread.sleep(2000)
  val initialFreeMemory = Server.freeMemory
  println(s"*** initially free: ${initialFreeMemory / 1000000}")
  println()
  for (n <- 0 until blocks) {
    ds.write(1, 1, -1, -1)(n * 1048576L, new Array[Byte](1048576))
  }
  System.gc()
  Thread.sleep(2000)
  val freeMemoryWhenStored = Server.freeMemory
  println(s"*** free when stored: ${freeMemoryWhenStored / 1000000}")
  println(s"*** used up when stored: ${(initialFreeMemory - freeMemoryWhenStored) / 1000000}")
  println(s"*** expected mem usage : $blocks")
  println()
  ds.delete(1, 1)
  System.gc()
  Thread.sleep(2000)
  val freeMemoryAfterDelete = Server.freeMemory
  println(s"*** free after delete: ${freeMemoryAfterDelete / 1000000}")
  println(s"*** used up after delete: ${(initialFreeMemory - freeMemoryAfterDelete) / 1000000}")
  println()

}

class DataStoreResearch(dataDir: String, readOnly: Boolean) extends AutoCloseable {
  private val initialFreeMemory: Long = Server.freeMemory
  private val log: Logger = LoggerFactory.getLogger(getClass)
  private val longTermStore = new LongTermStore(dataDir, readOnly)
  private val tempDir: File = new File(dataDir, "../data-temp").getAbsoluteFile
  require(tempDir.isDirectory && tempDir.list().isEmpty || tempDir.mkdirs())

  private var openChannels: Map[(Long, Long), SeekableByteChannel] = Map()
  private def path(id: Long, dataId: Long): Path = new File(tempDir, s"$id-$dataId").toPath
  private def channel(id: Long, dataId: Long): SeekableByteChannel = {
    require(!readOnly)
    openChannels.getOrElse(id -> dataId, {
      Files.newByteChannel(path(id, dataId), WRITE, CREATE_NEW, SPARSE, READ)
        .tap(channel => openChannels += ((id, dataId) -> channel))
    })
  }

  private sealed trait Entry {
    def id: Long; def dataId: Long
    def position: Long; def data: Array[Byte]
    def length: Int; def memory: Long
    def write(data: Array[Byte]): Unit
    def drop(left: Int, right: Int): Entry
    def ++(other: Entry): Entry
  }
  private object Entry {
    def apply(id: Long, dataId: Long, position: Long, data: Array[Byte]): Entry =
      MemoryEntry(id, dataId, position, data)
  }
  private case class MemoryEntry(id: Long, dataId: Long, position: Long, data: Array[Byte]) extends Entry {
    override def toString: String = s"Mem($id/$dataId, $position, $length)"
    override def length: Int = data.length
    override def memory: Long = length + 500
    override def drop(left: Int, right: Int): Entry = copy(position = position + left, data = data.drop(left).dropRight(right))
    override def write(data: Array[Byte]): Unit = System.arraycopy(data, 0, this.data, 0, data.length)
    override def ++(other: Entry): Entry = Entry(id, dataId, position, data ++ other.data)
  }
  private case class FileEntry(id: Long, dataId: Long, position: Long, length: Int) extends Entry {
    log.debug(s"create: $id/$dataId $position $length")
    override def toString: String = s"Fil($id/$dataId, $position, $length)"
    override def data: Array[Byte] = {
      log.debug(s"read  : $id/$dataId $position $length")
      val buffer = ByteBuffer.allocate(length)
      val input = channel(id, dataId).position(position)
      while(buffer.remaining() > 0) input.read(buffer)
      new Array[Byte](length).tap(buffer.position(0).get)
    }
    override def memory: Long = 500
    override def drop(left: Int, right: Int): Entry = {
      log.debug(s"drop  : $id/$dataId $position+$left $length-$left-$right")
      copy(position = position + left, length = length - left - right)
    }
    override def write(data: Array[Byte]): Unit = {
      log.debug(s"write : $id/$dataId $position $length data: ${data.length}")
      channel(id, dataId).position(position).write(ByteBuffer.wrap(data))
    }
    override def ++(other: Entry): Entry = {
      log.debug(s"++    : $id/$dataId $position $length $other")
      require(other.id == id && other.dataId == dataId)
      require(position + length == other.position)
      other match {
        case _: FileEntry => /* Nothing to do. */
        case _: MemoryEntry => channel(id, dataId).position(position + length).write(ByteBuffer.wrap(other.data))
      }
      copy(length = length + other.length)
    }
  }
  // Map(id/dataId -> size, Seq(data))
  private var entries = Map[(Long, Long), (Long, Seq[Entry])]()
  private var memoryUsage = 0L

  override def close(): Unit = {
    openChannels.keys.foreach { case (id, dataId) => delete(id, dataId) }
    longTermStore.close()
  }

  def hasTemporaryData(id: Long, dataId: Long): Boolean = entries.contains(id -> dataId)

  def read(id: Long, dataId: Long, ltStart: Long, ltStop: Long)(position: Long, requestedSize: Int): Array[Byte] = {
    val (fileSize, localEntries) = entries.getOrElse(id -> dataId, ltStop - ltStart -> Seq[Entry]())
    val sizeToRead = math.max(math.min(requestedSize, fileSize - position), 0).toInt
    val chunks: Seq[Entry] = localEntries.collect {
      case entry
        if (entry.position <  position && entry.position + entry.length > position) ||
           (entry.position >= position && entry.position                < position + sizeToRead) =>
        val dropLeft  = math.max(0, position - entry.position).toInt
        val dropRight = math.max(0, entry.position + entry.length - (position + sizeToRead)).toInt
        entry.drop(dropLeft, dropRight)
    }
    val chunksToRead = chunks.foldLeft(SortedMap(position -> sizeToRead)) { case (chunksToRead, entry) =>
      val (readPosition, sizeToRead) = chunksToRead.filter(_._1 <= entry.position).last
      chunksToRead - readPosition ++ Seq(
        readPosition -> (entry.position - readPosition).toInt,
        entry.position + entry.length -> (readPosition + sizeToRead - entry.position - entry.length).toInt
      ).filterNot(_._2 == 0)
    }
    val chunksRead: SortedMap[Long, Array[Byte]] =
      chunksToRead.map { case (start, length) => start -> longTermStore.read(ltStart + start, length) }
    (chunksRead ++ chunks.map(e => e.position -> e.data)).map(_._2).reduce(_ ++ _)
  }

  def size(id: Long, dataId: Long, ltStart: Long, ltStop: Long): Long =
    entries.get(id -> dataId).map(_._1).getOrElse(ltStop - ltStart)

  def delete(id: Long, dataId: Long, writeLog: Boolean = true): Unit = {
    if (writeLog) {
      log.info(s"*** memory usage: $memoryUsage")
      log.info(s"*** entries: ${entries.size}")
      log.info(s"*** entry: ${entries.head}")
      System.gc()
      Thread.sleep(2000)
      log.info(s"*** free: ${Server.freeMemory / 1000000}")
      System.gc()
      Thread.sleep(2000)
      log.info(s"*** free: ${Server.freeMemory / 1000000}")
    }
    memoryUsage = memoryUsage - entries.get(id -> dataId).toSeq.flatMap(_._2).map(_.memory).sum
    if (writeLog) {
      log.debug(s"Delete - memory usage $memoryUsage.")
      openChannels.get(id -> dataId).foreach { c =>
        c.close()
        Files.delete(path(id, dataId))
        log.debug(s"close : $id/$dataId")
      }
      openChannels -= id -> dataId
    }
    entries -= (id -> dataId)
  }

  def truncate(id: Long, dataId: Long, ltStart: Long, ltStop: Long): Unit = {
    delete(id, dataId, writeLog = false)
    log.debug(s"Truncate - memory usage $memoryUsage.")
    entries += (id -> dataId) -> (0L -> Seq())
  }

  var lastLog = 0L

  def write(id: Long, dataId: Long, ltStart: Long, ltStop: Long)(position: Long, data: Array[Byte]): Unit = {
    val (fileSize, chunks) = entries.getOrElse(id -> dataId, ltStop - ltStart -> Seq[Entry]())
    val newSize = math.max(fileSize, position + data.length)
    val combinedChunks = chunks :+ (chunks.find(_.position == position) match {
      case None => Entry(id, dataId, position, data)
      case Some(previousData) =>
        if (data.length >= previousData.length) Entry(id, dataId, position, data)
        else previousData.tap(_.write(data))
    })
    val (toMerge, others) = combinedChunks.partition { entry =>
      (entry.position < position && entry.position + entry.length > position) ||
        (entry.position >= position && entry.position < position + data.length)
    }
    val reduced = toMerge.sortBy(_.position).reduceLeft[Entry] { case (dataA, dataB) =>
      if (dataA.position + dataA.length >= dataB.position + dataB.length) dataA
      else dataA ++ dataB.drop((dataA.position + dataA.length - dataB.position).toInt, 0)
    }
    val merged = others :+ reduced
    delete(id, dataId, writeLog = false)
    memoryUsage += merged.map(_.memory).sum
    log.debug(s"Write - memory usage $memoryUsage.")
    entries += (id -> dataId) -> (newSize -> merged.map {
      case m: MemoryEntry => m.copy(data = m.data.toArray)
      case o => o
    })
  }

  def persist(id: Long, dataId: Long, ltStart: Long, ltStop: Long)(writePosition: Long, dataSize: Long): Unit = {
    for { offset <- 0L until dataSize by 32768; chunkSize = math.min(32768, dataSize - offset).toInt } {
      val chunk = read(id, dataId, ltStart, ltStop)(offset, chunkSize)
      longTermStore.write(writePosition + offset, chunk)
    }
  }
}
