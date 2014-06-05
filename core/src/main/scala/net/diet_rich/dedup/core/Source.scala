// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values.{Bytes, Size}
import net.diet_rich.dedup.util.valueOf

trait Source {
  def size: Size
  def read(count: Int): Bytes
  def allData: Iterator[Bytes] = new Iterator[Bytes] {
    var currentBytes = read(0x8000)
    def hasNext = currentBytes.length > 0
    def next = valueOf(currentBytes) before {currentBytes = read(0x8000)}
  }
  def reset: Unit
  def close: Unit
}
