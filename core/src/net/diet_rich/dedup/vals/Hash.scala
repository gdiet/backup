// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.vals

import net.diet_rich.util.vals.ByteArrayValue
import net.diet_rich.util.vals.Bytes
import java.security.MessageDigest

case class Hash(value: Array[Byte]) extends ByteArrayValue { import java.util.Arrays
  override def equals(a: Any) = a match {
    case null => false
    case Hash(other) => Arrays.equals(value, other)
    case _ => false
  }
  override def hashCode() = Arrays.hashCode(value)
}

object Hash {
  type BytesSource = {
    def next(numberOfBytes: Int): Bytes
  }
  
  def filter[U](algorithm: String)(source: BytesSource)(sink: BytesSource => U): (Hash, U) = {
    val digest = MessageDigest getInstance algorithm
    val u = sink(new {
      def next(numberOfBytes: Int): Bytes = {
        val bytes = source next numberOfBytes
        digest update (bytes data, bytes offset, bytes length)
        bytes
      }
    })
    (Hash(digest.digest()), u)
  }
}
