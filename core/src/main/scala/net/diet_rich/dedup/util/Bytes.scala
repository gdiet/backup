// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.util

sealed trait Bytes {
  def data: Array[Byte]
  def offset: Int
  def length: Int

  def withOffset(off: Int) = {
    assume(off <= length)
    Bytes(data, offset + off, length - off)
  }
}

object Bytes {
  def apply(length: Int): Bytes = new SimpleBytes(new Array[Byte](length))
  def apply(data: Array[Byte], offset: Int, length: Int): Bytes = new FullBytes(data, offset, length)
  def unapply(bytes: Bytes) = Some((bytes.data, bytes.offset, bytes.length))

  private class SimpleBytes(val data: Array[Byte]) extends Bytes {
    val offset = 0
    def length = data.length
  }

  private class FullBytes(val data: Array[Byte], val offset: Int, val length: Int) extends Bytes

  import scala.language.reflectiveCalls
  implicit class UpdateBytes(val u: { def update(data: Array[Byte], offset: Int, length: Int) }) extends AnyVal {
    def update(bytes: Bytes) = u.update(bytes.data, bytes.offset, bytes.length)
  }
  implicit class SetInputBytes(val u: { def setInput(data: Array[Byte], offset: Int, length: Int) }) extends AnyVal {
    def setInput(bytes: Bytes) = u.setInput(bytes.data, bytes.offset, bytes.length)
  }
}
