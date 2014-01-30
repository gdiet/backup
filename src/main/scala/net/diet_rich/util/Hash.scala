// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import java.security.MessageDigest

case class Hash(value: Array[Byte]) {
  def !==(a: Hash) = ! ===(a)
  def ===(a: Hash) = java.util.Arrays.equals(value, a.value)
  override def equals(a: Any) = ???
}

object Hash {
  def checkAlgorithm(algorithm: String): String = {
    MessageDigest getInstance algorithm
    algorithm
  }
  def getLength(algorithm: String): Int =
    MessageDigest getInstance algorithm getDigestLength
  def instance(algorithm: String) =
    MessageDigest getInstance algorithm
  def forEmptyData(algorithm: String): Hash =
    Hash(MessageDigest getInstance algorithm digest)
}
