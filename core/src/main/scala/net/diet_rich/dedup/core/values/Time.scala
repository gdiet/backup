// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

case class Time(val value: Long) extends LongValue

object Time extends (Long => Time) {
  def apply(value: Option[Long]): Option[Time] = value map Time
}