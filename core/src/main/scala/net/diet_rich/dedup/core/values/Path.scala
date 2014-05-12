// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

case class Path(value: String) {
  import Path._
  assume((value == ROOTNAME) || (value startsWith SEPARATOR), s"Path <$value> is not root and does not start with '$SEPARATOR'")

  def +(string: String) = Path(value + string)
  def parent: Path =
    value.lastIndexOf('/') match {
      case -1 => throw new IllegalArgumentException(s"Can't get parent for path '$value'")
      case n  => Path(value.substring(0, n))
    }
  def name: String = value.substring(value.lastIndexOf('/') + 1)
}

object Path extends (String => Path) {
  val SEPARATOR = "/"
  val ROOTNAME = ""
  val ROOTPATH = Path(ROOTNAME)
}
