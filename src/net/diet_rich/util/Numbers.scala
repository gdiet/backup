// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import net.diet_rich.util.vals.LongValue

object Numbers {
  def toInt(value: Long): Int = {
    assume(value <= Int.MaxValue && value >= Int.MinValue, s"value $value is not an Int")
    value toInt
  }
  def toInt(value: LongValue): Int = toInt(value.value)
}