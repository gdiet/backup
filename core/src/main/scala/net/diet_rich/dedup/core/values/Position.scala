// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

case class Position(val value: Long) extends LongValue with Ordered[Position] {
  def +(size: Size): Position = Position(value + size.value)
  def block(blocksize: Size): Long = (value / blocksize.value)
  def blockOffset(blocksize: Size): Size = Size(value % blocksize.value)
  override def compare(that: Position): Int = value compareTo that.value
}
