// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.vals

case class Bytes (data: Array[Byte], offset: Int, length: Int) {
  override def equals(a: Any) = throw new NotImplementedError
  override def hashCode() = throw new NotImplementedError
  def take(number: Int) = copy(length = number)
  def drop(number: Int) = copy(data, offset + number, length - number)
  def isEmpty = offset + length == data.length
  def isFull = offset == 0 && length == data.length
}

object Bytes {
  def apply(length: Int): Bytes = Bytes(new Array(length), 0, length)
  def empty = Bytes(0)
}
