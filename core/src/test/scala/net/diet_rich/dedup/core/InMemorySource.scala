// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.util.valueOf

import net.diet_rich.dedup.core.values.{Size, Bytes}

class InMemorySource(data: Bytes) extends ResettableSource {
  private var position: Int = data.offset
  def size: Size = data.size
  def read(count: Int): Bytes = {
    val length = math.min(count, data.length - position)
    valueOf(data.copy(offset = position, length = length)) before { position = position + length }
  }
  def reset: Unit = { position = data.offset }
  def close = Unit
}