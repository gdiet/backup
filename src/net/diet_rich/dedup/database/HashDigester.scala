// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.util.Hashes
import net.diet_rich.util.io._

class HashDigester(algorithm: String) {
  def filterHash[ReturnType](input: Reader)(reader: Reader => ReturnType): (Hash, ReturnType) = {
    val digester = Hashes.instance(algorithm)
    val wrappedInput = new Object {
      def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
        val size = input.read(bytes, offset, length)
        if (size > 0) digester.update(bytes, offset, size)
        size
      }
      def close(): Unit = input.close
    }
    val returned = reader(wrappedInput)
    (Hash(digester.digest()), returned)
  }
}