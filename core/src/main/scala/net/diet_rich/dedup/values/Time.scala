// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.values

case class Time(val value: Long) extends LongValue

object Time {
  def apply(value: Option[Long]): Option[Time] = value map Time.apply
}
