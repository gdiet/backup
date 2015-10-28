package net.diet_rich.dedupfs.metadata

import org.specs2.Specification

import net.diet_rich.bytestore.file.FileBackend
import net.diet_rich.common.test._

class FreeRangesSpec extends Specification {
  def is = sequential ^ s2"""
With a block size of 1000 and initial ranges of 100-2500, 8000-max, 4800-5000,
the free ranges object should
  ${eg{ freeRanges.pushBack((2600L, 2700L)) === unit }}
  $checkNextSevenRanges
"""
  val freeRanges = new FreeRanges(Seq(
    100L -> 2500L,
    8000L -> Long.MaxValue,
    4800L -> 5000L
  ), FileBackend.nextBlockStart(1000L, _))

  def checkNextSevenRanges =
    List.fill(7)(freeRanges.nextBlock) === List(
      (2600L, 2700L),
      ( 100L, 1000L),
      (1000L, 2000L),
      (2000L, 2500L),
      (4800L, 5000L),
      (8000L, 9000L),
      (9000L,10000L)
    )
}
