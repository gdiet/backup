// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.vals

import net.diet_rich.util.Numbers.OverflowProtectedOps

case class Size(value: Long) extends OrderedLongValue[Size] {
  def +(other: Long): Size = copy(value +# other)
  def +(other: Size): Size = copy(value +# other.value)
  def -(other: Size): Size = copy(value -# other.value)
}
object Size {
  def min(a: Size, b: Size): Size = if (a < b) a else b
  def max(a: Size, b: Size): Size = if (a > b) a else b
}
