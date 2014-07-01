// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.util.init

class RangesQueue {
  // Note: We could use a PriorityQueue here - however, it is not really necessary,
  // in our use case, an ordinary queue 'heals' itself here, too.
  private val freeRangesQueue = scala.collection.mutable.Queue[DataRange]()

  def dequeue(size: Size): List[DataRange] = freeRangesQueue synchronized {
    @annotation.tailrec
    def collectFreeRanges(size: Size, ranges: List[DataRange]): List[DataRange] = {
      freeRangesQueue dequeue() partitionAt size match {
        case WithRest(range, rest) => freeRangesQueue enqueue rest; range :: ranges
        case ExactMatch(range) => range :: ranges
        case NeedsMore(range, remainingSize) => collectFreeRanges(remainingSize, range :: ranges)
      }
    }
    if (size isZero) Nil else collectFreeRanges(size, Nil)
  }

  def enqueue(range: DataRange) = freeRangesQueue synchronized { freeRangesQueue enqueue range }
}

object RangesQueue {
  def apply(initialRange: DataRange): RangesQueue = init(new RangesQueue) {_.enqueue(initialRange)}
}
