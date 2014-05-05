// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.values

case class TreeEntryType(value: Int) extends IntValue {
  assume(0 <= value && value <= 1, s"Unsupported tree node type $value")
}

object TreeEntryType {
  val DIR = TreeEntryType(0)
  val FILE = TreeEntryType(1)
}
