// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.databaseXXX

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
    val sizeOfRange = blockSize - (startOfFreeArea % blockSize)
    (copy(startOfFreeArea = startOfFreeArea + sizeOfRange), Range(startOfFreeArea, startOfFreeArea + sizeOfRange))
  } else
    (copy(freeSlices = freeSlices.tail), freeSlices.head)
}

object FreeRanges {
  def hasOverlappingSlices(slices: Iterable[Range]): Boolean =
    slices.size > 1 &&
    slices.toList.sorted.sliding(2).exists {
      case List(Range(_, Position(end1)), Range(Position(start2), _)) => 
        end1 > start2
    }
  def hasSlicesAcrossBlocks(slices: Iterable[Range], blockSize: IntSize) =
    slices.exists {
      case Range(start, end) =>
        start / blockSize != (end - Size(1)) / blockSize
    }
  def hasSlicesInFreeArea(slices: Iterable[Range], startOfFreeArea: Position) =
    slices.exists {
      case Range(_, end) => end > startOfFreeArea
    }
}