// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

case class TreeEntryID(value: Long)
object TreeEntryID {
  def apply(value: Option[Long]): Option[TreeEntryID] = value map TreeEntryID.apply
}

case class DataEntryID(value: Long)
object DataEntryID {
  def apply(value: Option[Long]): Option[DataEntryID] = value map DataEntryID.apply
}

case class Print(value: Long)

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
  require(0 <= value && value <= 1, s"Unsupported tree node type $value")
}
object NodeType {
  val DIR = NodeType(0)
  val FILE = NodeType(1)
}

case class Method(value: Int) {
  require(0 <= value && value <= 1, s"Unsupported store method $value")
}
object Method {
  val STORE = Method(0)
  val DEFLATE = Method(1)
}
