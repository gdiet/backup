// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.vals._
import net.diet_rich.util.vals._

trait Reference {
  def time: Time
  def size: Size
  def print: Print
  def dataid: DataEntryID
}
