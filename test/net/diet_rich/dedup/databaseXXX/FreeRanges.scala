// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.databaseXXX

import java.sql.Connection
import net.diet_rich.dedup.database.SqlDBUtil.ValuesFromSqlResult // FIXME shorter import?
import net.diet_rich.util.sql._
import net.diet_rich.util.vals._

case class FreeRanges(
  blockSize: IntSize,
  startOfFreeArea: Position, 
  private val freeSlices: Set[Range]
) {
  import FreeRanges._
  assume (!hasOverlappingSlices(freeSlices), "overlapping slices")
  assume (!hasSlicesAcrossBlocks(freeSlices, blockSize), "slices across blocks")
  assume (!hasSlicesInFreeArea(freeSlices, startOfFreeArea), "slices in free area")
  def add(range: Range): FreeRanges = {
    assume (!freeSlices.contains(range), "slice already present")
    copy(freeSlices = freeSlices + range)
  }
  def get: (FreeRanges, Range) = if (freeSlices.isEmpty) {
    val newSlice = sliceAtStartOfFreeArea(startOfFreeArea, blockSize)
    assume (!isSliceAcrossBlocks(blockSize)(newSlice), "new slice across blocks")
    (copy(startOfFreeArea = newSlice.end), newSlice)
  } else
    (copy(freeSlices = freeSlices.tail), freeSlices.head)
}

object FreeRanges {
  def sliceAtStartOfFreeArea(startOfFreeArea: Position, blockSize: IntSize) =
    Range(startOfFreeArea, startOfFreeArea + blockSize - startOfFreeArea % blockSize)
  def hasOverlappingSlices(slices: Iterable[Range]): Boolean =
    slices.size > 1 &&
    slices.toList.sorted.sliding(2).exists {
      case List(Range(_, Position(end1)), Range(Position(start2), _)) => 
        end1 > start2
    }
  def hasSlicesAcrossBlocks(slices: Iterable[Range], blockSize: IntSize) =
    slices exists isSliceAcrossBlocks(blockSize)
  def isSliceAcrossBlocks(blockSize: IntSize)(slice: Range) =
    slice.start / blockSize != (slice.end - Size(1)) / blockSize
  def hasSlicesInFreeArea(slices: Iterable[Range], startOfFreeArea: Position) =
    slices.exists {
      case Range(_, end) => end > startOfFreeArea
    }
  
  def startOfFreeAreaInDB(implicit connection: Connection) =
    // java.sql.ResultSet: if the value is SQL NULL, the value returned is 0
    execQuery("SELECT MAX(fin) FROM ByteStore")(_ position 1).next
  def freeSlicesInDB(implicit connection: Connection): Set[Range] = {
    // the first entry is the start of the data area
    val dataStarts = execQuery(
      "SELECT b1.start FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.start = b2.fin WHERE b2.fin IS NULL ORDER BY b1.start"
    )(_ position 1).toList
    // the last entry is the start of the free area
    val dataEnds = execQuery(
      "SELECT b1.fin FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.fin = b2.start WHERE b2.start IS NULL ORDER BY b1.fin"
    )(_ position 1).toList
    dataStarts match {
      case Nil =>
        assume(dataEnds.isEmpty)
        Set()
      case dataStart :: startTail =>
        assume(dataEnds.reverse.head == startOfFreeAreaInDB, s"dataEnd does not match startOfFreeAreaInDB")
        val bodyEntries = dataEnds.zip(startTail) // tail for one offset between the lists
        val bodySet = bodyEntries.toSet.map(Range.tupled)
        if (dataStart == Position(0)) bodySet else bodySet + Range(Position(0), dataStart)
    }
  }
}