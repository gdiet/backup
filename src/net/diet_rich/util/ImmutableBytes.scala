// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

/** The bytes array contained is immutable by contract: Create Bytes 
 *  objects only for byte arrays that need not be changed any more.
 */
class ImmutableBytes (val bytes: Array[Byte], val size: Int) {
  assume(size >= 0, "size %s must be greater or equal 0" format size)
  assume(size <= bytes.length, "size %s must be less or equal array length %s" format (size, bytes.length))
  def isFull = size == bytes.length
}

object ImmutableBytes {
  def apply(data: Array[Byte], size: Int) = new ImmutableBytes(data, size)
  def apply(data: Array[Byte]) = new ImmutableBytes(data, data.length)
}
