// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.util

class Size(val value: Long) extends AnyVal {
  def +  (other: Long): Size    = Size(value + other)
  def <  (other: Long): Boolean = value <  other
  def <= (other: Size): Boolean = value <= other.value
  def toInt: Int = {
    require (value <= Int.MaxValue && value >= Int.MinValue)
    value toInt
  }
}
object Size { def apply(value: Long) = new Size(value) }

class Time(val value: Long) extends AnyVal
object Time { def apply(value: Long) = new Time(value) }
