// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

import org.specs2.SpecificationWithJUnit

class DataRangeSpec  extends SpecificationWithJUnit { def is = s2"""
${"Tests for data range partitioning".title}

Partitioning should work correctly at boundary $boundary
Partitioning should work correctly at boundary+1 $boundaryPlusOne
  """

  def boundary = {
    val range = DataRange(Position(10), Position(20))
    val (start, rest) = range.partitionAtBlockLimit(Size(20))
    (start should beEqualTo(range)) and
      (rest should beEmpty)
  }

  def boundaryPlusOne = {
    val range = DataRange(Position(10), Position(21))
    val (start, rest) = range.partitionAtBlockLimit(Size(20))
    (start should beEqualTo(DataRange(Position(10), Position(20)))) and
      (rest should beEqualTo(Some(DataRange(Position(20), Position(21)))))
  }

}
