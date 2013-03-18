// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.util.Hashes
import net.diet_rich.util.io._
import net.diet_rich.util.vals._

class HashDigester(algorithm: String) {
  def filterHash[ReturnType](source: ByteSource)(processor: ByteSource => ReturnType): (Hash, ReturnType) = {
    val digester = Hashes.instance(algorithm)
    val wrappedInput = new Object {
      def read(bytes: Array[Byte], offset: Position, length: Size): Size = {
        val size = source.read(bytes, offset, length)
        if (size > Size(0)) digester.update(bytes, offset.intValue, size.intValue)
        size
      }
    }
    val returned = processor(wrappedInput)
    (Hash(digester.digest()), returned)
  }
}