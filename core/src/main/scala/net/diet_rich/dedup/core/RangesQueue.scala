// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values.{DataRange, Size}
import net.diet_rich.dedup.util.init

trait FreeRangesSlice {
  def freeRanges: RangesQueue
}

trait FreeRangesPart extends FreeRangesSlice { _: sql.SessionSlice with data.BlockSizeSlice =>
  override final val freeRanges = {
    val freeInData = if (sql.DBUtilities.problemDataAreaOverlaps.isEmpty) sql.DBUtilities.freeRangesInDataArea else Nil
    new RangesQueue(dataBlockSize, freeInData ::: List(sql.DBUtilities.freeRangeAtEndOfDataArea))
  }
}

class RangesQueue(blocksize: Size, initialRanges: Seq[DataRange]) {
  // Note: We could use a PriorityQueue here - however, it is not really necessary,
  // because here, an ordinary queue 'heals', quickly getting into the right order.
  private val freeRangesQueue = init(scala.collection.mutable.Queue[DataRange]())(_.enqueue(initialRanges:_*))

  /** Dequeues the required size plus padding up to the next end-of-block. */
  def dequeueAtLeast(size: Size): List[DataRange] = freeRangesQueue synchronized {
    @annotation.tailrec
    def collectFreeRanges(size: Size, ranges: List[DataRange]): List[DataRange] = {
      import DataRange._
      freeRangesQueue dequeue() limitAt size match {
        case NotLargeEnough(range, remainingSize) => collectFreeRanges(remainingSize, range :: ranges)
        case ExactMatch(range) => range :: ranges
        case IsLarger(range) =>
          val alreadyUsedInTheLastBlock = (range.start + size) % blocksize
          val offsetToABlockBorder = if (alreadyUsedInTheLastBlock.isZero) Size.Zero else blocksize - alreadyUsedInTheLastBlock
          val sizeToTheBlockBorder = size + offsetToABlockBorder
          if (sizeToTheBlockBorder < range.size) {
            freeRangesQueue enqueue range.withOffset(sizeToTheBlockBorder)
            range.withLength(sizeToTheBlockBorder) :: ranges
          } else
            range :: ranges
      }
    }
    if (size.isZero) Nil else collectFreeRanges(size, Nil)
  }

  def enqueue(range: DataRange): Unit = freeRangesQueue synchronized { freeRangesQueue enqueue range }
  def enqueue(ranges: Seq[DataRange]): Unit = freeRangesQueue synchronized { freeRangesQueue.enqueue(ranges:_*) }
}
