// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.vals

class Size(val value: Long) extends AnyVal {
  def +  (other: Long): Size    = Size(value + other)
  def -  (other: Size): Size    = Size(value - other.value)
  def <  (other: Size): Boolean = value <  other.value
  def >  (other: Size): Boolean = value >  other.value
  def <= (other: Size): Boolean = value <= other.value
}
object Size { def apply(value: Long) = new Size(value) }

class Position(val value: Long) extends AnyVal {
  def + (other: Size): Position = Position(value + other.value)
  def - (other: Position): Size = Size(value - other.value)
  def compare (other: Position): Int = value compare other.value
}
object Position { def apply(value: Long) = new Position(value) }

class Time(val value: Long) extends AnyVal
object Time { def apply(value: Long) = new Time(value) }
