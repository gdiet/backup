// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import java.sql.Connection
import net.diet_rich.dedup.datastore.StoreMethods
import net.diet_rich.util.io._
import net.diet_rich.util.sql._
import net.diet_rich.util.vals._
import net.diet_rich.util.Numbers

import SqlDBUtil.ValuesFromSqlResult

// FIXME try to eliminate the .toInt calls

// FIXME make blockSize an IntSize
class FreeRanges(blockSize: Int)(implicit connection: Connection) {
  // EVENTUALLY, it would be good to look for illegal overlaps:
  // SELECT * FROM ByteStore b1 JOIN ByteStore b2 ON b1.start < b2.fin AND b1.fin > b2.fin
  // Illegal overlaps should be ignored during free space detection

  // EVENTUALLY, it would be good to look for orphan ByteStore entries.
  // Emit warning and free the space?

  case class DataRange(start: Position, fin: Position) extends Ordered[DataRange] {
    assume(fin-start <= Size(blockSize))
    assume(fin-start >  Size(0))
    assume(start.value / blockSize == (fin.value-1) / blockSize)
    def intLength = Numbers.toInt(length)
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
  
  
  // Note: Initially, needs at least an "empty" entry in table.
  private val startOfFreeArea = execQuery("SELECT MAX(fin) FROM ByteStore")(_ position 1).next
  private val queue = new scala.collection.mutable.PriorityQueue[DataRange]()
  
  // insert ranges for gaps in queue
  private val gapStarts = execQuery(
    "SELECT b1.fin FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.fin = b2.start WHERE b2.start IS NULL ORDER BY b1.fin"
  )(_ position 1).filterNot(_ == startOfFreeArea)
  private val gapEndsAndDataStart = execQuery(
    "SELECT b1.start FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.start = b2.fin WHERE b2.fin IS NULL ORDER BY b1.start"
  )(_ position 1)
  private val dataStart = gapEndsAndDataStart.nextOption
  
  // insert space between 0 and data start if any
  dataStart.filterNot(_ == Position(0)).foreach(e => queue.enqueue(DataRange(Position(0), e)))
  // insert gaps
  gapStarts.zip(gapEndsAndDataStart).foreach(e => queue.enqueue(DataRange(e._1, e._2)))
  // insert upper free area
  enqueueUpperEnd(startOfFreeArea)

  def next: DataRange = queue.synchronized {
    val range = queue.dequeue()
    if (queue.isEmpty) {
      assume(range.fin.value % blockSize == 0)
      enqueueUpperEnd(range.fin)
    }
    range
  }
  def enqueue(range: DataRange) = if (range.length > Size(0))
    queue.synchronized { queue.enqueue(range) }
  def enqueueUpperEnd(position: Position) = queue.synchronized {
    val length = Size(blockSize - position.value % blockSize)
    queue.enqueue(DataRange(position, position + length))
  }
}


trait ByteStoreTable {
  implicit def connection: Connection
  
  private val freeRanges = new FreeRanges(ds.dataSize)
  import freeRanges.DataRange

  private val maxEntryId =
    SqlDBUtil.readAsAtomicLong(
      "SELECT GREATEST((SELECT MAX(id) FROM DataInfo), (SELECT MAX(dataid) FROM ByteStore))"
    )

  def read(dataId: DataEntryID, method: Method): ByteSource =
    StoreMethods.wrapRestore(read(dataId), method)
  private def read(dataId: DataEntryID): ByteSource = new Object {
    // FIXME process as list-like or stream or ...?
    val parts = selectEntryParts(dataId)(r => DataRange(r position 1, r position 2))
    var rangeOpt: Option[DataRange] = None
    def read(bytes: Array[Byte], offset: Int, length: Int): Int =
      if (rangeOpt.isEmpty || rangeOpt.get.isEmpty) {
        if (!parts.hasNext) 0 else {
          rangeOpt = Some(parts.next)
          read(bytes, offset, length)
        }
      } else {
        val range = rangeOpt.get
        val rangeLength = Numbers.toInt(range.length.value)
        val bytesToRead = math.min(rangeLength, length)
        val read = ds.readFromSingleDataFile(range.start, bytes, offset, bytesToRead)
        rangeOpt = if (read == rangeLength) None else Some(range.withOffset(Size(read)))
        read
      }
  }
  protected final val selectEntryParts = 
    prepareQuery("SELECT start, fin FROM ByteStore WHERE dataid = ? ORDER BY index ASC")
    
  def storeAndGetDataId(bytes: Array[Byte], size: Size, method: Method): DataEntryID =
    storeAndGetDataIdAndSize(new java.io.ByteArrayInputStream(bytes, 0, Numbers.toInt(size)), method)._1
  
  def storeAndGetDataIdAndSize(source: ByteSource, method: Method): (DataEntryID, Size) = {
    val sourceToWrite = new Object {
      var totalRead = 0L
      def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
        val localRead = source.read(bytes, offset, length)
        if (localRead > 0) totalRead = totalRead + localRead
        localRead
      }
    }
    val possiblyCompressedSource = StoreMethods.wrapStore(sourceToWrite, method)
    val dataEntryId = storeAndGetDataId(possiblyCompressedSource)
    val sizeWritten = Size(sourceToWrite.totalRead)
    (dataEntryId, sizeWritten)
  }

  private def storeAndGetDataId(source: ByteSource): DataEntryID = {
    val id = DataEntryID(maxEntryId incrementAndGet())
    @annotation.tailrec
    def writeStep(range: DataRange, index: Int): Unit = {
      writeRange(id, index, source, range) match {
        case e if e.isEmpty =>
          writeStep(freeRanges.next, index + 1)
        case remaining =>
          freeRanges.enqueue(remaining)
      }
    }
    writeStep(freeRanges.next, 0)
    id
  }

  private def writeRange(id: DataEntryID, index: Int, source: ByteSource, range: DataRange): DataRange = {
    val bytes = new Array[Byte](32768)
    @annotation.tailrec
    def writeStep(range: DataRange, offsetInArray: Int, dataInArray: Int, alreadyRead: Size): Size = {
      if (range.length == Size(0)) {
        alreadyRead
      } else if (dataInArray == 0) {
        val bytesToRead = if (range.length < Size(bytes.length)) range.length.value.toInt else bytes.length
        fillFrom(source, bytes, 0, bytesToRead) match {
          case 0 => alreadyRead
          case read => writeStep(range, 0, read, alreadyRead + Size(read))
        }
      } else {
        ds.writeNewDataToSingleDataFile(range.start, bytes, offsetInArray, dataInArray)
        writeStep(range.withOffset(Size(dataInArray)), 0, 0, alreadyRead)
      }
    }
    val size = writeStep(range, 0, 0, Size(0))
    if (size > Size(0)) insertEntry(id, index, range.start, range.start + size)
    range.withOffset(size)
  }
  protected final val insertEntry = 
    prepareSingleRowUpdate("INSERT INTO ByteStore (dataid, index, start, fin) VALUES (?, ?, ?, ?)")

  // implemented in other pieces of cake
  protected val ds: net.diet_rich.dedup.datastore.DataStore2
}

object ByteStoreTable {
  def createTable(implicit connection: Connection) : Unit = {
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
