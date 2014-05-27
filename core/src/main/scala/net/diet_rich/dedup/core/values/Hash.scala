// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

import java.security.MessageDigest

import net.diet_rich.dedup.util.Bytes

case class Hash(value: Array[Byte]) {
  def !==(a: Hash) = ! ===(a)
  def ===(a: Hash) = java.util.Arrays.equals(value, a.value)
  override def equals(a: Any) = throw new UnsupportedOperationException("use === to compare hash contents")
  override def toString = s"Hash(${value map ("%02X" format _) mkString})"
}

object Hash extends (Array[Byte] => Hash) {

  def algorithmChecked(algorithm: String): String = {
    MessageDigest getInstance algorithm
    algorithm
  }

  def digestLength(algorithm: String): Int =
    MessageDigest getInstance algorithm getDigestLength

  def calculate(algorithm: String, data: Iterator[Bytes]): (Hash, Size) = {
    val digester = MessageDigest getInstance algorithm
    val numberOfBytes = data map { bytes => digester update bytes; bytes.length.toLong } sum;
    (Hash(digester.digest), Size(numberOfBytes))
  }

  def calculate[T](algorithm: String, data: Iterator[Bytes], withBytes: Bytes => T): (Hash, Size, List[T]) = {
    val digester = MessageDigest getInstance algorithm
    val (result, sizes) = data.map { bytes => digester update bytes; (withBytes(bytes), bytes.length.toLong) }.toList.unzip
    (Hash(digester.digest), Size(sizes.sum), result)
  }
  
}
