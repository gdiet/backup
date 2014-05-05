// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.values

case class TreeEntryID(value: Long) extends LongValue

object TreeEntryID {
  def apply(value: Option[Long]): Option[TreeEntryID] = value map TreeEntryID.apply
}
