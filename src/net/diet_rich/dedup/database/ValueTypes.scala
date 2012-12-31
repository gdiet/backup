// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

class TreeEntryID(val value: Long) extends AnyVal
object TreeEntryID { def apply(value: Long) = new TreeEntryID(value) }

class DataEntryID(val value: Long) extends AnyVal
object DataEntryID {
  def apply(value: Long) = new DataEntryID(value)
  def apply(value: Option[Long]) = value.map(new DataEntryID(_))
}

class Print(val value: Long) extends AnyVal
object Print { def apply(value: Long) = new Print(value) }

class Hash(val value: Array[Byte]) extends AnyVal
object Hash { def apply(value: Array[Byte]) = new Hash(value) }
