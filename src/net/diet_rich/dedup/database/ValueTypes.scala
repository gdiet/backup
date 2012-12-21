// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

class TreeEntryID(val value: Long) extends AnyVal
object TreeEntryID { def apply(value: Long) = new TreeEntryID(value) }

class DataEntryID(val value: Long) extends AnyVal
object DataEntryID { def apply(value: Long) = new DataEntryID(value) }

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

class Print(val value: Long) extends AnyVal
object Print { def apply(value: Long) = new Print(value) }

class Hash(val value: Array[Byte]) extends AnyVal
object Hash { def apply(value: Array[Byte]) = new Hash(value) }
