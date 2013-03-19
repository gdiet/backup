// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.util.vals._

case class TreeEntryID(value: Long) extends LongValue
object TreeEntryID extends ApplyLongOption[TreeEntryID]

case class DataEntryID(value: Long) extends LongValue
object DataEntryID extends ApplyLongOption[DataEntryID]

case class Print(value: Long) extends LongValue {
  def ^(other: Print): Print = copy(value ^ other.value)
}

case class Hash(value: Array[Byte]) extends ByteArrayValue { import java.util.Arrays
  override def equals(a: Any) = a match {
  	case null => false
  	case h: Hash => Arrays.equals(value, h.value)
  	case _ => false
  }
  override def hashCode() = Arrays.hashCode(value)
}

case class Path(value: String) {
  def +(string: String) = Path(value + string)
  def parent: Path =
    value.lastIndexOf('/') match {
      case -1 => throw new IllegalArgumentException(s"Can't get parent for path '$value'")
      case n  => Path(value.substring(0, n))
    }
  def name: String = value.substring(value.lastIndexOf('/') + 1)
}

case class NodeType(value: Long) extends LongValue {
  require(NodeType.ALLOWED contains value, s"Unsupported tree node type $value")
}
object NodeType {
  private val ALLOWED = Set(0L, 1L)
  val DIR = NodeType(0)
  val FILE = NodeType(1)
}

case class Method(value: Long) extends LongValue {
  require(Method.ALLOWED contains value, s"Unsupported store method $value")
}
object Method {
  private val ALLOWED = Set(0L, 1L)
  val STORE = Method(0)
  val DEFLATE = Method(1)
}
