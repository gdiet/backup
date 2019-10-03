package dedup2

import java.lang.Runtime.{getRuntime => runtime}
import java.lang.System.{arraycopy, currentTimeMillis => now}
import java.util.Arrays.copyOf

import org.slf4j.LoggerFactory

import scala.collection.immutable.SortedMap

object MemoryStore {
  private def freeMemory = runtime.maxMemory() - runtime.totalMemory() + runtime.freeMemory()
  private def available = math.max(runtime.maxMemory() * 8 / 10, freeMemory)

  type Entries = SortedMap[Long, Array[Byte]]
  val emptyMap = SortedMap[Long, Array[Byte]]()(Ordering.Long.reverse)
  type Entry = (Long, Array[Byte])

  /** Merge if data and entryData overlap, else return None. */
  def merge(start: Long, data: Array[Byte], entryStart: Long, entryData: Array[Byte]): Option[Entry] = {
    val end = start + data.length
    val entryEnd = entryStart + entryData.length
    if (end >= entryStart && entryStart == entryEnd) Some(start -> data) // eliminate non-terminating empty entry
    else if (start >= entryEnd || end <= entryStart) None // no overlap
    else if (start >= entryStart && end <= entryEnd) { // data fits into entry
      arraycopy(data, 0, entryData, (start - entryStart).toInt, data.length)
      Some(entryStart -> entryData)
    } else if (start <= entryStart && end >= entryEnd) { // entry fits into data
      Some(start -> data)
    } else if (start < entryStart) { // data overlaps entry to the left
      val resultSize = (entryEnd - start).toInt
      val array = copyOf(data, resultSize)
      val startInEntry = entryData.length + data.length - resultSize
      arraycopy(entryData, startInEntry, array, data.length, entryData.length - startInEntry)
      Some(start -> array)
    } else { // data overlaps entry to the right
      val resultSize = (end - entryStart).toInt
      val array = copyOf(entryData, resultSize)
      arraycopy(data, 0, array, resultSize - data.length, data.length)
      Some(entryStart -> array)
    }
  }

  /** Merge new data array into entries vector. */
  def merge(start: Long, data: Array[Byte], entries: Entries): Entries = {
    (entries.foldLeft((emptyMap, start -> data)) {
      case ((map, (start, data)), (entryStart, entryData)) =>
        merge(start, data, entryStart, entryData) match {
          case None => (map + (entryStart -> entryData), start -> data)
          case Some(merged) => map -> merged
        }
    }).pipe { case (map, last) => map + last }
  }
}

class MemoryStore { import MemoryStore._
  private val log = LoggerFactory.getLogger(getClass)
  private var storage: Map[Long, Entries] = Map() // dataid -> entries
  private var lastOutput = now

  override def toString: String = s"MemoryStore${
    storage.map { case (dataId, entries) =>
      s"[$dataId -> ${ entries.map { case (start, data) => s"$start:${data.length}::${data.take(10).mkString(",")}" }.mkString("  ") }]"
    }.mkString("(", ", ", ")")
  }"

  def size(dataId: Long): Option[Long] = synchronized {
    storage.get(dataId).flatMap(_.headOption.map { case (start, data) => start + data.length })
  }

  def store(dataId: Long, start: Long, data: Array[Byte]): Boolean = synchronized {
    if (lastOutput + 1000 < now) { log.debug(s"available memory: ${available/1000000}MB"); lastOutput = now }
    if (available < data.length) { storage -= dataId; false }
    else {
      storage += dataId -> (storage.get(dataId) match {
        case None => emptyMap + (start -> data)
        case Some(entries) => merge(start, data, entries)
      })
      true
    }
  }

  def read(dataId: Long, offset: Long, intSize: Int): Array[Byte] = synchronized {
    storage.get(dataId) match {
      case None => Array()
      case Some(entries) =>
        val fileSize = entries.headOption.map { case (start, data) => start + data.length }.getOrElse(0L)
        val dataSize = math.min(intSize, fileSize - offset).toInt
        if (dataSize < 1) Array() else {
          val result = new Array[Byte](dataSize)
          entries.exists { case (start, data) =>
            if (start + data.length <= offset) true
            else {
              if (start < offset) {
                val startIndexInData = (offset - start).toInt
                val bytesToCopy = math.min(data.length - startIndexInData, dataSize)
                arraycopy(data, startIndexInData, result, 0, bytesToCopy)
              } else if (start < offset + intSize) {
                val startIndexInResult = (start - offset).toInt
                val bytesToCopy = math.min(data.length, dataSize - startIndexInResult)
                arraycopy(data, 0, result, startIndexInResult, bytesToCopy)
              }
              false
            }
          }
          result
        }
    }
  }
}
