package net.diet_rich.dedup.core.meta

import scala.language.reflectiveCalls

import org.specs2.Specification

import net.diet_rich.dedup.core.data.file.FileBackend
import net.diet_rich.dedup.util.init

class RangesSpec extends Specification { def is = s2"""
${"Tests for the free ranges queue".title}

Fetching free ranges should return the correct results
  when ranges have been pushed back $afterPushBack
"""

  val ranges = List(
    ( 100L, 2500L),
    (4800L, 5000L),
    (8000L, Long.MaxValue)
  )

  def afterPushBack = {
    val freeRanges = new FreeRanges(ranges, FileBackend.nextBlockStart(_, 1000L))
    List((20L, 50L), (3800L, 4200L)) foreach freeRanges.pushBack
    List.fill(8)(freeRanges.nextBlock) ===
      List(
        (3800L, 4000L),
        (4000L, 4200L),
        (  20L,   50L),
        ( 100L, 1000L),
        (1000L, 2000L),
        (2000L, 2500L),
        (4800L, 5000L),
        (8000L, 9000L)
      )
  }
}
