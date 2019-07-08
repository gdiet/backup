package util

import java.security.MessageDigest
import scala.util.chaining._

object Hash {
  def apply(algorithm: String, bytes: Array[Byte], length: Int): Array[Byte] =
    MessageDigest.getInstance(algorithm).pipe { md => md.update(bytes, 0, length); md.digest() }
}
