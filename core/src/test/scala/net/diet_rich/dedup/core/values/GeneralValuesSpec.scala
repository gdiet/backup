// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

import org.specs2.SpecificationWithJUnit

class GeneralValuesSpec extends SpecificationWithJUnit { def is = s2"""
${"Tests for the value classes".title}

Size must have a correctly ascending ordering $sizeIsAscending
Position must have a correctly ascending ordering $positionIsAscending
  """

  def sizeIsAscending = Size(1) should beGreaterThan(Size(0))
  def positionIsAscending = Position(1) should beGreaterThan(Position(0))
}
