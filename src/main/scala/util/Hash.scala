package util

import java.security.MessageDigest

object Hash {
  def apply(algorithm: String, bytes: Array[Byte]): Array[Byte] =
    MessageDigest.getInstance(algorithm).digest(bytes)
}
