// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

// FIXME tests for ordering of size and position
case class Size(val value: Long) extends LongValue with Ordered[Size] {
  def -(other: Size): Size = Size(value - other.value)
  override def compare(that: Size): Int = value compareTo that.value
}
