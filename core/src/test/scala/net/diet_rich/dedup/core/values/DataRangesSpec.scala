// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

import org.specs2.SpecificationWithJUnit

class DataRangesSpec extends SpecificationWithJUnit { def is = s2"""
${"Tests for normalizing data ranges".title}

Data ranges should be normalized correctly in the case of
  empty data range $empty
  data range with some normalization parts $filled
"""

  def empty = List.empty[DataRange].normalize should beEmpty

  private def r(start: Int, end: Int) = DataRange(Position(start), Position(end))

  def filled =
    List(r(1,3), r(4,6), r(6,8), r(10,20), r(20,30), r(30,40), r(44,48)).normalize should beEqualTo(
      List(r(1,3), r(4,8), r(10,40), r(44,48))
    )
}
