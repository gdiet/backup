// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.util

import net.diet_rich.dedup.core.values.Size

trait Source {
  def size: Size
  def read(count: Int): Bytes
  def allData: Iterator[Bytes]
  def reset: Unit
  def close
}