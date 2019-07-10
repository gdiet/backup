package util

import java.security.MessageDigest

object Hash {
  def apply(algorithm: String, bytes: Seq[Array[Byte]]): (Long, Array[Byte]) = {
    val md = MessageDigest.getInstance(algorithm)
    val totalSize = bytes.foldLeft(0L){ case (size, chunk) => md.update(chunk); size + chunk.length }
    totalSize -> md.digest
  }
}
