// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.vals

import net.diet_rich.util.vals._

case class Print(value: Long) extends LongValue {
  def ^(other: Print): Print = copy(value ^ other.value)
}
