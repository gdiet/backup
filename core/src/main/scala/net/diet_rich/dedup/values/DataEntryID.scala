// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.values

case class DataEntryID(value: Long) extends LongValue

object DataEntryID {
  def apply(value: Option[Long]): Option[DataEntryID] = value map DataEntryID.apply
}
