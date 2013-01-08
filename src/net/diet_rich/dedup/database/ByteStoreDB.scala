// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.util.io._
import net.diet_rich.util.sql._
import net.diet_rich.util.vals._

case class DataRange(start: Position, fin: Position) extends Ordered[DataRange] {
  def length = fin - start
  def isEmpty = fin == start
  def withLength(length: Size) = copy(fin = start + length)
  def withOffset(offset: Size) = copy(start = start + offset)
  override def compare(that: DataRange): Int =
    that.start compare start match {
      case 0 => that.fin compare fin
      case x => x
    }
}
object DataRange {
  val emptyRange = DataRange(Position(0), Position(0))
}

class FreeRanges(implicit connection: WrappedConnection) {
  // EVENTUALLY, it would be good to look for illegal overlaps:
  // SELECT * FROM ByteStore b1 JOIN ByteStore b2 ON b1.start < b2.fin AND b1.fin > b2.fin
  // Illegal overlaps should be ignored during free space detection

  // EVENTUALLY, it would be good to look for orphan ByteStore entries.
  // Emit warning and free the space?
  
  // Note: Initially, needs at least an "empty" entry in table.
  private val blockSize = Size(32000000)
  private val startOfFreeArea = execQuery("SELECT MAX(fin) FROM ByteStore")(_ long 1).next
  private val queue = new scala.collection.mutable.PriorityQueue[DataRange]()
  
  // insert ranges for gaps in queue
  private val gapStarts = execQuery(
    "SELECT b1.fin FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.fin = b2.start WHERE b2.start IS NULL ORDER BY b1.fin"
  )(_ long 1).filterNot(_ == startOfFreeArea)
  private val gapEndsAndDataStart = execQuery(
    "SELECT b1.start FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.start = b2.fin WHERE b2.fin IS NULL ORDER BY b1.start"
  )(_ long 1)
  private val dataStart = gapEndsAndDataStart.nextOption
  
  // insert space between 0 and data start if any
  dataStart.filterNot(_ == 0).foreach(e => queue.enqueue(DataRange(Position(0), Position(e))))
  // insert gaps
  gapStarts.zip(gapEndsAndDataStart).foreach(e => queue.enqueue(DataRange(Position(e._1), Position(e._2))))
  // insert upper free area
  queue.enqueue(DataRange(Position(startOfFreeArea), Position(Long.MaxValue)))

  def next: DataRange = queue.synchronized {
    val range = queue.dequeue()
    if (range.fin == Position(Long.MaxValue)) {
      queue.enqueue(range.withOffset(blockSize))
      range.withLength(blockSize)
    } else range
  }
  def enqueue(range: DataRange) = if (range.length > Size(0))
    queue.synchronized { queue.enqueue(range) }
}


trait ByteStoreDB {
  implicit def connection: WrappedConnection
  
  private val freeRanges = new FreeRanges

  private val maxEntryId =
    SqlDBUtil.readAsAtomicLong(
      "SELECT GREATEST((SELECT MAX(id) FROM DataInfo), (SELECT MAX(dataid) FROM ByteStore))"
    )
  
  def storeAndGetDataId(bytes: Array[Byte], size: Size): DataEntryID = {
    val id = DataEntryID(maxEntryId incrementAndGet())
    @annotation.tailrec
    def writeStep(index: Int, offset: Position, length: Size): Unit = if (length > Size(0)) {
      val range = freeRanges.next
      if (length > range.length) {
        writeToStore(range.start, bytes, offset, range.length)
        insertEntry(id.value, index, range.start.value, range.fin.value)
        writeStep(index + 1, offset + range.length, length - range.length)
      } else {
        writeToStore(range.start, bytes, offset, length)
        insertEntry(id.value, index, range.start.value, range.start.value + length.value)
        freeRanges.enqueue(range.withOffset(length))
      }
    }
    writeStep(0, Position(0), size)
    id
  }

  def storeAndGetDataIdAndSize(reader: Reader): (DataEntryID, Size) = {
    val id = DataEntryID(maxEntryId incrementAndGet())
    @annotation.tailrec
    def writeStep(range: DataRange, index: Int, size: Size): Size = {
      writeRange(id, index, reader, range) match {
        case e if e.isEmpty =>
          writeStep(freeRanges.next, index + 1, size + range.length)
        case remaining =>
          freeRanges.enqueue(remaining)
          size + range.length - remaining.length
      }
    }
    (id, writeStep(freeRanges.next, 0, Size(0)))
  }

  private def writeRange(id: DataEntryID, index: Int, reader: Reader, range: DataRange): DataRange = {
    val bytes = new Array[Byte](32768)
    @annotation.tailrec
    def writeStep(range: DataRange, offsetInArray: Position, dataInArray: Size, alreadyRead: Size): Size = {
      if (range.length == 0) {
        alreadyRead
      } else if (dataInArray == Size(0)) {
        val bytesToRead = if (range.length < Size(bytes.length)) range.length.value.toInt else bytes.length
        fillFrom(reader, bytes, 0, bytesToRead) match {
          case 0 => alreadyRead
          case read => writeStep(range, Position(0), Size(read), alreadyRead + Size(read))
        }
      } else {
        writeToStore(range.start, bytes, offsetInArray, dataInArray)
        writeStep(range.withOffset(offsetInArray asSize), Position(0), Size(0), alreadyRead)
      }
    }
    val size = writeStep(range, Position(0), Size(0), Size(0))
    insertEntry(id.value, index, range.start.value, range.start.value + size.value)
    range.withOffset(size)
  }
  protected final val insertEntry = 
    prepareSingleRowUpdate("INSERT INTO ByteStore (dataid, index, start, fin) VALUES (?, ?, ?, ?)")

  
  // implemented in other pieces of cake
  def writeToStore(position: Position, bytes: Array[Byte], offset: Position, size: Size): Unit
}

object ByteStoreDB {
  def createTable(implicit connection: WrappedConnection) : Unit = {
    // index: data part index (starts at 0)
    // start: data part start position
    // fin: data part end position + 1
    execUpdate("""
      CREATE CACHED TABLE ByteStore (
        dataid BIGINT NOT NULL,
        index  INTEGER NOT NULL,
        start  BIGINT NOT NULL,
        fin    BIGINT NOT NULL
      );
    """);
    execUpdate("CREATE INDEX idxByteStoreData ON ByteStore(dataid);")
    execUpdate("CREATE INDEX idxByteStoreStart ON ByteStore(start);")
    execUpdate("CREATE INDEX idxByteStoreFin ON ByteStore(fin);")
  }
}
