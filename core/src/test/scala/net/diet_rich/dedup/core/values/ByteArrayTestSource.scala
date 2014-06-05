// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

import net.diet_rich.dedup.core.Source
import net.diet_rich.dedup.util.valueOf

class ByteArrayTestSource(data: Array[Byte]) extends Source {
  private var position: Int = 0
  def size: Size = Size(data.length)
  def read(count: Int): Bytes = {
    val length = math.min(count, data.length - position)
    valueOf(Bytes(data, position, length)) before { position = position + length }
  }
  def reset: Unit = { position = 0 }
  def close = Unit
}
