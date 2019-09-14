package dedup2

import java.lang.Runtime.{getRuntime => runtime}
import java.lang.System.{arraycopy, currentTimeMillis => now}
import java.util.Arrays.copyOf

object MemoryStore {
  type Entry = (Long, Array[Byte])
  private def freeMemory = runtime.maxMemory() - runtime.totalMemory() + runtime.freeMemory()
  private def available = math.max(runtime.maxMemory() * 8 / 10, freeMemory)

  /** Merge if data and entryData overlap, else return None. */
  def merge(start: Long, data: Array[Byte], entryStart: Long, entryData: Array[Byte]): Option[Entry] = {
    val end = start + data.length
    val entryEnd = entryStart + entryData.length
    if (start >= entryEnd || end <= entryStart) None // no overlap
    else if (start >= entryStart && end <= entryEnd) { // data fits into entry
      arraycopy(data, 0, entryData, (start - entryStart).toInt, data.length)
      Some(entryStart -> entryData)
    } else if (start <= entryStart && end >= entryEnd) { // entry fits into data
      Some(start -> copyOf(data, data.length))
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
  def merge(start: Long, data: Array[Byte], entries: Vector[Entry]): Vector[Entry] = {
    val (vector, merged) = entries.foldLeft((Vector[Entry](), start -> data)) {
      case ((vector, (start, data)), (entryStart, entryData)) =>
        merge(start, data, entryStart, entryData) match {
          case None => (vector :+ entryStart -> entryData, start -> data)
          case Some(merged) => vector -> merged
        }
    }
    vector :+ merged
  }
}

class MemoryStore { import MemoryStore._
  private var storage: Map[Long, Vector[Entry]] = Map() // dataid -> entries
  private var lastOutput = now

  override def toString: String = s"MemoryStore${
    storage.map { case (dataId, entries) =>
      s"[$dataId -> ${ entries.map { case (start, data) => s"$start:${data.length}::${data.take(10).mkString(",")}" }.mkString("  ") }]"
    }.mkString("(", ", ", ")")
  }"

  def store(dataId: Long, start: Long, data: Array[Byte]): Boolean = synchronized {
    if (lastOutput + 1000 < now) { println(s"available: $available"); lastOutput = now }
    if (available < data.length) { storage -= dataId; false }
    else {
      storage += dataId -> (storage.get(dataId) match {
        case None => Vector(start -> copyOf(data, data.length))
        case Some(entries) => merge(start, data, entries)
      })
      true
    }
  }
}
