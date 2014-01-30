// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.util.Hash
import net.diet_rich.util.io._

class HashDigester(algorithm: String) {
  def filterHash[ReturnType](source: ByteSource)(processor: ByteSource => ReturnType): (Hash, ReturnType) = {
    val digester = Hash instance algorithm
    val wrappedInput = new Object {
      def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
        val size = source.read(bytes, offset, length)
        if (size > 0) digester.update(bytes, offset, size)
        size
      }
    }
    val returned = processor(wrappedInput)
    (Hash(digester digest), returned)
  }
}