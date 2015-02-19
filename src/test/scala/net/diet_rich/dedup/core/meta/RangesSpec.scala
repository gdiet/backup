package net.diet_rich.dedup.core.meta

import net.diet_rich.dedup.core.data.file.FileBackend
import org.specs2.Specification

class RangesSpec extends Specification { def is = s2"""
${"Tests for the free ranges queue".title}

Fetching free ranges should return the correct results for
 size 0 $size0
 a part of the first free range where the next block border is within the free range $firstBlockInRange
A part of the first free range where the next block border is beyond the free range
The full first free range
A part of the second free range
A part of the last free range not at a block border
A part of the last free range at a block border
"""

  val ranges = List(
    ( 100L, 3500L),
    (4800L, 5000L),
    (8000L, Long.MaxValue)
  )
  def queue = new RangesQueue(ranges, FileBackend.nextBlockStart(_, 1000L))

  def size0 = queue.dequeueAtLeast(0) should beEmpty
  def firstBlockInRange = queue.dequeueAtLeast(1000) should contain(exactly((100L, 2000L)))
}
