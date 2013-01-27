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
  def apply1(value: Long) = new DataEntryID(value)
  def apply2(value: Option[Long]) = value.map(new DataEntryID(_))
}

class Print(val value: Long) extends AnyBase with ValueToString
object Print { def apply(value: Long) = new Print(value) }

class Hash(val value: Array[Byte]) { // FIXME not a value class anymore
  def !==(a: Hash) = ! ===(a)
  def ===(a: Hash) = java.util.Arrays.equals(value, a.value)
  override def equals(a: Any) = ???
}
object Hash { def apply(value: Array[Byte]) = new Hash(value) }

class Path(val value: String) extends AnyBase with ValueToString {
  def +(string: String) = Path(value + string)
}
object Path { def apply(value: String) = new Path(value) }

class NodeType(val value: Int) extends AnyBase with ValueToString
object NodeType {
  val DIR = NodeType(0)
  val FILE = NodeType(1)
  def apply(value: Int): NodeType = {
    require(0 <= value && value <=1, s"Unsupported tree node type $value")
    new NodeType(value)
  }
}

class Method(val value: Int) extends AnyBase with ValueToString
object Method {
  val STORE = Method(0)
  val DEFLATE = Method(1)
  def apply(value: Int): Method = {
    val result = new Method(value)
    require(0 <= value && value <=1, s"Unsupported store method $value")
    result
  }
}
