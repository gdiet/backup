// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

case class Size(val value: Long) extends LongValue with Ordered[Size] {
  def -(other: Size): Size = Size(value - other.value)
  def +(other: Size): Size = Size(value + other.value)
  def isZero: Boolean = value == 0L
  override def compare(that: Size): Int = value compareTo that.value
}
