// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

case class Bytes(bytes: Array[Byte], length: Int, offset: Int = 0) {
  require(offset >= 0)
  require(length >= 0)
  require(bytes.length >= offset + length)
  def dropFirst(size: Int) = copy(length = length - size, offset = offset + size)
  def keepFirst(size: Int) = copy(length = size)
}

object Bytes {
  def apply(bytes: Array[Byte]) : Bytes = Bytes(bytes, bytes.length)
  def apply(size: Int) : Bytes = Bytes(new Array[Byte](size))
}
