// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values.{DataRange, Size}
import net.diet_rich.dedup.util.init

trait FreeRangesSlice {
  def freeRanges: RangesQueue
}

trait FreeRangesPart { _: sql.SessionSlice =>
  final val freeRanges = {
    val freeInData = if (sql.DBUtilities.problemDataAreaOverlaps isEmpty) sql.DBUtilities.freeRangesInDataArea else Nil
    RangesQueue(freeInData ::: List(sql.DBUtilities.freeRangeAtEndOfDataArea))
  }
}

class RangesQueue {
  // Note: We could use a PriorityQueue here - however, it is not really necessary,
  // in our use case, an ordinary queue 'heals' itself here, too.
  private val freeRangesQueue = scala.collection.mutable.Queue[DataRange]()

  def dequeue(size: Size): List[DataRange] = freeRangesQueue synchronized {
    @annotation.tailrec
    def collectFreeRanges(size: Size, ranges: List[DataRange]): List[DataRange] = {
      import DataRange._
      freeRangesQueue dequeue() limitAt size match {
        case NotLargeEnough(range, remainingSize) => collectFreeRanges(remainingSize, range :: ranges)
        case ExactMatch(range) => range :: ranges
        case IsLarger(range, rest) => freeRangesQueue enqueue rest; range :: ranges
      }
    }
    if (size isZero) Nil else collectFreeRanges(size, Nil)
  }

  def enqueue(range: DataRange): Unit = freeRangesQueue synchronized { freeRangesQueue enqueue range }
  def enqueue(ranges: Seq[DataRange]): Unit = freeRangesQueue synchronized { freeRangesQueue.enqueue(ranges:_*) }
}

object RangesQueue {
  def apply(initialRange: DataRange): RangesQueue = init(new RangesQueue) {_.enqueue(initialRange)}
  def apply(initialRanges: Seq[DataRange]): RangesQueue = init(new RangesQueue) {_.enqueue(initialRanges)}
}
