// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.util.vals.{AnyBase, ValueToString}

class TreeEntryID(val value: Long) extends AnyBase with ValueToString
object TreeEntryID {
  def apply(value: Long) = new TreeEntryID(value)
  def apply(value: Option[Long]): Option[TreeEntryID] = value.map(TreeEntryID(_))
}

class DataEntryID(val value: Long) extends AnyBase with ValueToString
object DataEntryID {
  def apply(value: Long) = new DataEntryID(value)
  def apply(value: Option[Long]) = value.map(new DataEntryID(_))
}

class Print(val value: Long) extends AnyBase with ValueToString
object Print { def apply(value: Long) = new Print(value) }

class Hash(val value: Array[Byte]) extends AnyBase
object Hash { def apply(value: Array[Byte]) = new Hash(value) }

class Path(val value: String) extends AnyBase {
  def +(string: String) = Path(value + string)
}
object Path { def apply(value: String) = new Path(value) }
