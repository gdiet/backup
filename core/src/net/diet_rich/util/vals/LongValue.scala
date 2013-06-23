// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.vals

trait LongValue extends TypedValue[Long] {
  def intValue = {
    assume(value <= Int.MaxValue && value >= Int.MinValue, s"value $value is not an Int")
    value toInt
  }
}
