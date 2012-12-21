package net.diet_rich.backup.database

import net.diet_rich.util.sql._
import java.sql.Connection
import net.diet_rich.util.io.Reader

trait ByteStoreDB {
  def storeAndGetDataId(bytes: Array[Byte], size: Long): Long
  def storeAndGetDataIdAndSize(reader: Reader): (Long, Long)
}

case class DataRange(start: Long, fin: Long) extends Ordered[DataRange] {
  def length = fin - start
  def withLength(length: Long) = copy(fin = start + length)
  def withOffset(offset: Long) = copy(start = start + offset)
  override def compare(that: DataRange): Int =
    that.start compare start match {
      case 0 => that.fin compare fin
      case x => x
    }
}

// initially, needs at least an "empty" entry in table.
class FreeRanges(implicit connection: Connection) {
  private val blockSize = 100000000
  private val startOfFreeArea = execQuery(connection, "SELECT MAX(fin) FROM ByteStore")(_ long 1).next
  private val queue = new scala.collection.mutable.PriorityQueue[DataRange]()
  
  // insert ranges for gaps in queue
  private val gapStarts = execQuery(connection,
    "SELECT b1.fin FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.fin = b2.start WHERE b2.start IS NULL ORDER BY b1.fin"
  )(_ long 1).filterNot(_ == startOfFreeArea)
  private val gapEndsAndDataStart = execQuery(connection,
    "SELECT b1.start FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.start = b2.fin WHERE b2.fin IS NULL ORDER BY b1.start"
  )(_ long 1)
  private val dataStart = gapEndsAndDataStart.nextOption
  
  // insert space between 0 and data start if any
  dataStart.filterNot(_ == 0).foreach(e => queue.enqueue(DataRange(0, e)))
  // insert gaps
  gapStarts.zip(gapEndsAndDataStart).foreach(e => queue.enqueue(DataRange(e._1, e._2)))
  // insert upper free area
  queue.enqueue(DataRange(startOfFreeArea, Long.MaxValue))
  
  def next: DataRange = queue.synchronized {
    val range = queue.dequeue()
    if (range.fin == Long.MaxValue) {
      queue.enqueue(range.withOffset(blockSize))
      range.withLength(blockSize)
    } else range
  }
  def enqueue(range: DataRange) =
    queue.synchronized { queue.enqueue(range) }
}


trait BasicByteStoreDB extends ByteStoreDB {
  implicit def connection: Connection
  
  def ??? = throw new UnsupportedOperationException // FIXME ???

  def write(position: Long, data: Array[Byte], offset: Int, length: Int): Unit
  
  private val freeRanges = new FreeRanges
  
  protected final val insertEntry = 
    prepareUpdate("INSERT INTO ByteStore (dataid, index, start, fin) VALUES (?, ?, ?, ?)")
  protected def write(dataid: Long)(writeFunction: DataRange => Long): Unit = {
    @annotation.tailrec
    def writeStep(index: Int): Unit = {
      val range = freeRanges.next
      val stored = writeFunction(range)
      if (stored > 0) {
        insertEntry(dataid, index, range.start, range.start + stored)
        // if the range was not fully used, re-enqueue the remainder
        if (stored < range.length) freeRanges.enqueue(range.withOffset(stored))
        writeStep(index+1)
      }
      // if the range was not used up, re-enqueue it
      else freeRanges.enqueue(range)
    }
    writeStep(0)
  }
  
  def storeAndGetDataId(bytes: Array[Byte], size: Long): Long = {
    def writeStep(index: Int, offset: Int, length: Int): Unit = {
      ???
    }
    ???
  }
  
}
