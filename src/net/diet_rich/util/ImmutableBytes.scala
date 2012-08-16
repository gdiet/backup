// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

/** The bytes array contained is immutable by contract: Create Bytes 
 *  objects only for byte arrays that need not be changed any more.
 */
class ImmutableBytes (val data: Array[Byte], val size: Int) {
  def isFull = size == data.length
}

object ImmutableBytes {
  def apply(data: Array[Byte], size: Int) = new ImmutableBytes(data, size)
}
