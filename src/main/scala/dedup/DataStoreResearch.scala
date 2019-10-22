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

class DataStoreResearch() {
  private val log: Logger = LoggerFactory.getLogger(getClass)

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
  // Map(id/dataId -> size, Seq(data))
  private var entries = Map[(Long, Long), (Long, Seq[Entry])]()
  private var memoryUsage = 0L

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
    if (writeLog) log.debug(s"Delete - memory usage $memoryUsage.")
    entries -= (id -> dataId)
  }

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
}
