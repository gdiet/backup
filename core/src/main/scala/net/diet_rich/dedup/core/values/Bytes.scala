// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

case class Bytes(data: Array[Byte], offset: Int, length: Int) {
  def size: Size = Size(length)
  def withOffset(offset: Size): Bytes = withOffset(offset.value.toInt)
  def withOffset(off: Int): Bytes = {
    assume(off >= 0 && off <= length)
    Bytes(data, offset + off, length - off)
  }
}

object Bytes extends ((Array[Byte], Int, Int) => Bytes) {
  def empty(length: Int): Bytes = Bytes(new Array[Byte](length), 0, 0)
  def zero(length: Int): Bytes = Bytes(new Array[Byte](length), 0, length)

  import scala.language.reflectiveCalls
  implicit class UpdateBytes(val u: { def update(data: Array[Byte], offset: Int, length: Int) }) extends AnyVal {
    def update(bytes: Bytes) = u.update(bytes.data, bytes.offset, bytes.length)
  }
  implicit class SetInputBytes(val u: { def setInput(data: Array[Byte], offset: Int, length: Int) }) extends AnyVal {
    def setInput(bytes: Bytes) = u.setInput(bytes.data, bytes.offset, bytes.length)
  }
}
