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

trait ByteStoreTable {
  implicit def connection: Connection
  
  private var freeRanges = // FIXME ds.dataSize as IntSize
    FreeRanges(ds.dataSize, FreeRanges.startOfFreeAreaInDB, FreeRanges.freeSlicesInDB)
  private def getDataSlice: Range = synchronized {
    val (newFreeRanges, slice) = freeRanges.get
    freeRanges = newFreeRanges
    slice
  }
  private def putDataSlice(slice: Range) = synchronized {
    freeRanges = freeRanges.add(slice)
  }
  
  private val maxEntryId =
    execQuery(
      "SELECT GREATEST((SELECT MAX(id) FROM DataInfo), (SELECT MAX(dataid) FROM ByteStore))"
    )(_ atomicLong 1).nextOnly

  def read(dataId: DataEntryID, method: Method): ByteSource =
    StoreMethods.wrapRestore(read(dataId), method)
  private def read(dataId: DataEntryID): ByteSource = new Object {
    // FIXME process as list-like or stream or ...?
    val parts = selectEntryParts(dataId)(r => Range(r position 1, r position 2))
    var rangeOpt: Option[Range] = None
    def read(bytes: Array[Byte], offset: Int, length: Int): Int =
      if (rangeOpt.isEmpty) {
        if (!parts.hasNext) 0 else {
          rangeOpt = Some(parts.next)
          read(bytes, offset, length)
        }
      } else {
        val range = rangeOpt.get
        val rangeLength = Numbers.toInt(range.length.value)
        val bytesToRead = Size(math.min(rangeLength, length))
        val read = ds.readFromSingleDataFile(range.start, bytes, Position(offset), bytesToRead)
        rangeOpt = if (read.value == rangeLength) None else Some(range +/ read)
        read.intValue
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
    def writeStep(range: Range, index: Int): Unit = {
      writeRange(id, index, source, range) match {
        case None =>
          writeStep(getDataSlice, index + 1)
        case Some(remaining) =>
          putDataSlice(remaining)
      }
    }
    writeStep(getDataSlice, 0)
    id
  }

  private def writeRange(id: DataEntryID, index: Int, source: ByteSource, range: Range): Option[Range] = {
    val bytes = new Array[Byte](32768)
    @annotation.tailrec
    def writeStep(range: Range, offsetInArray: Position, dataInArray: Size, alreadyRead: Size): Size = {
      if (range.length == Size(0)) {
        alreadyRead
      } else if (dataInArray == Size(0)) {
        val bytesToRead = if (range.length < Size(bytes.length)) range.length.value.toInt else bytes.length
        fillFrom(source, bytes, 0, bytesToRead) match {
          case 0 => alreadyRead
          case read => writeStep(range, Position(0), Size(read), alreadyRead + Size(read))
        }
      } else {
        ds.writeNewDataToSingleDataFile(range.start, bytes, offsetInArray, dataInArray)
        writeStep(range +/ dataInArray, Position(0), Size(0), alreadyRead)
      }
    }
    val size = writeStep(range, Position(0), Size(0), Size(0))
    if (size > Size(0)) insertEntry(id, index, range.start, range.start + size)
    if (size == range.length) None else Some(range +/ size)
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
