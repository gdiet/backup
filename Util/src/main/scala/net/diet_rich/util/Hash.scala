package net.diet_rich.util

import java.security.MessageDigest

object Hash {
  def digestLength(algorithm: String): Int = MessageDigest getInstance algorithm getDigestLength()
  def empty(algorithm: String): Array[Byte] = MessageDigest getInstance algorithm digest()
}
