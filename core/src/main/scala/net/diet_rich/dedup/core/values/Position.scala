// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

case class Position(value: Long) extends LongValue with Ordered[Position] {
  def +(size: Size): Position = Position(value + size.value)
  def -(size: Size): Position = Position(value - size.value)
  def -(other: Position): Size = Size(value - other.value)
  def %(size: Size): Size = Size(value % size.value)
  def /(size: Size): Long = value / size.value
  override def compare(that: Position): Int = value compareTo that.value
}
