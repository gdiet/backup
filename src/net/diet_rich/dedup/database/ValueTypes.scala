// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.util.vals._

case class TreeEntryID(value: Long) extends LongValue
object TreeEntryID {
  def apply(value: Option[Long]): Option[TreeEntryID] = value.map(TreeEntryID(_))
}

case class DataEntryID(value: Long) extends LongValue
object DataEntryID {
  def apply(value: Option[Long]): Option[DataEntryID] = value.map(DataEntryID(_))
}

case class Print(value: Long) extends LongValue

class Hash(val value: Array[Byte]) { // FIXME not a value class anymore
  def !==(a: Hash) = ! ===(a)
  def ===(a: Hash) = java.util.Arrays.equals(value, a.value)
  override def equals(a: Any) = ???
}
object Hash { def apply(value: Array[Byte]) = new Hash(value) }

case class Path(value: String) {
  def +(string: String) = Path(value + string)
  def parent: Path =
    value.lastIndexOf('/') match {
      case -1 => throw new IllegalArgumentException(s"Can't get parent for path '$value'")
      case n  => Path(value.substring(0, n))
    }
  def name: String = value.substring(value.lastIndexOf('/') + 1)
}

case class NodeType(value: Int) {
  require(NodeType.ALLOWED contains value, s"Unsupported tree node type $value")
}
object NodeType {
  private val ALLOWED = Set(0, 1)
  val DIR = NodeType(0)
  val FILE = NodeType(1)
}

case class Method(value: Int) {
  require(Method.ALLOWED contains value, s"Unsupported store method $value")
}
object Method {
  private val ALLOWED = Set(0, 1)
  val STORE = Method(0)
  val DEFLATE = Method(1)
}
