package net.diet_rich.dedup.core.meta

import scala.language.reflectiveCalls

import org.specs2.Specification

import net.diet_rich.dedup.core.data.file.FileBackend
import net.diet_rich.dedup.util.init

class RangesSpec extends Specification { def is = s2"""
${"Tests for the free ranges queue".title}

Fetching free ranges should return the correct results for
  size 0 $size0
  a part of the first free range where the next block border is within the free range $firstBlockInRange
  a part of the first free range where the next block border is beyond the free range $firstBlockBeyondRange
  a part of the first free range up to a block border $firstBlockAtBlockBorder
  the full first free range $fullFirstRange
  a part of the second free range $secondRange
  a part of the last free range not at a block border $lastRangeBetweenBorders
  a part of the last free range at a block border $lastRangeAtBorder
The ranges queue should have the correct internal state after dequeueing
  a part of the full first block $stateAfterDequeueSmallPartOfFirstBlock
  most part of the full first range $stateAfterDequeueLargePartOfFirstRange
  the full first range $stateAfterDequeueFirstRange
  almost up to a block border $stateAfterDequeueAlmostToBlockBorder
  up to a block border $stateAfterDequeueToBlockBorder
"""

  val ranges = List(
    ( 100L, 3500L),
    (4800L, 5000L),
    (8000L, Long.MaxValue)
  )
  def queue = new RangesQueue(ranges, FileBackend.nextBlockStart(_, 1000L)) {
    def ranges = freeRangesQueue
  }


  def size0 = queue.dequeueAtLeast(0) should beEmpty
  def firstBlockInRange = queue.dequeueAtLeast(1000) should contain(exactly((100L, 2000L)))
  def firstBlockBeyondRange = queue.dequeueAtLeast(3000) should contain(exactly((100L, 3500L)))
  def firstBlockAtBlockBorder = queue.dequeueAtLeast(2900) should contain(exactly((100L, 3000L)))
  def fullFirstRange = queue.dequeueAtLeast(3400) should contain(exactly((100L, 3500L)))
  def secondRange = queue.dequeueAtLeast(3500) should contain(exactly((100L, 3500L), (4800L, 5000L)) inOrder)
  def lastRangeBetweenBorders = queue.dequeueAtLeast(4000) should contain(exactly((100L, 3500L), (4800L, 5000L), (8000L, 9000L)) inOrder)
  def lastRangeAtBorder = queue.dequeueAtLeast(4600) should contain(exactly((100L, 3500L), (4800L, 5000L), (8000L, 9000L)) inOrder)
  def stateAfterDequeueFirstRange = init(queue){_.dequeueAtLeast(3400)}.ranges === ranges.tail
  def stateAfterDequeueSmallPartOfFirstBlock = init(queue){_.dequeueAtLeast(1)}.ranges === (1000L, 3500L) :: ranges.tail
  def stateAfterDequeueLargePartOfFirstRange = init(queue){_.dequeueAtLeast(3000)}.ranges === ranges.tail
  def stateAfterDequeueAlmostToBlockBorder = init(queue){_.dequeueAtLeast(3500)}.ranges === ranges.tail.tail
  def stateAfterDequeueToBlockBorder = init(queue){_.dequeueAtLeast(3600)}.ranges === ranges.tail.tail
}
