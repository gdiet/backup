// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

import scala.collection.mutable.MutableList

import net.diet_rich.dedup.util.valueOf

case class Bytes(data: Array[Byte], offset: Int, length: Int) {
  def size: Size = Size(length)
  def withSize(size: Size): Bytes = withSize(size.value toInt)
  def withSize(length: Int): Bytes = {
    assume(length > 0 && length <= this.length) // or length <= data.length - offset ?
    copy(length = length)
  }
  def withOffset(offset: Size): Bytes = withOffset(offset.value toInt)
  def withOffset(off: Int): Bytes = {
    assume(off >= 0 && off <= length)
    Bytes(data, offset + off, length - off)
  }
}

object Bytes extends ((Array[Byte], Int, Int) => Bytes) {
  val EMPTY = empty(0)

  // Note: MutableList allows in-place replacement when applying the store method to minimize memory impact
  def consumingIterator(data: MutableList[Bytes]): Iterator[Bytes] = new Iterator[Bytes] {
    var index = 0
    def hasNext: Boolean = index < data.size
    def next: Bytes = valueOf(data(index)) before {
      data.update(index, EMPTY)
      index += 1
    }
  }

  def empty(length: Int): Bytes = Bytes(new Array[Byte](length), 0, 0)
  def zero(length: Int): Bytes = Bytes(new Array[Byte](length), 0, length)
  def zero(size: Size): Bytes = zero(size.value toInt)

  implicit class SizeOfBytesList(val data: Iterable[Bytes]) extends AnyVal {
    def totalSize: Size = data.map(_.size).foldLeft(Size.Zero)(_+_)
  }
  
  import scala.language.reflectiveCalls
  implicit class UpdateBytes(val u: { def update(data: Array[Byte], offset: Int, length: Int) }) extends AnyVal {
    def update(bytes: Bytes) = u.update(bytes.data, bytes.offset, bytes.length)
  }
  implicit class SetInputBytes(val u: { def setInput(data: Array[Byte], offset: Int, length: Int) }) extends AnyVal {
    def setInput(bytes: Bytes) = u.setInput(bytes.data, bytes.offset, bytes.length)
  }
}
