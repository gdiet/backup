package net.diet_rich.common

import java.security.MessageDigest

object Hash {
  def digestLength(algorithm: String): Int = MessageDigest getInstance algorithm getDigestLength()
  def empty(algorithm: String): Array[Byte] = MessageDigest getInstance algorithm digest()

  /** @return Hash and byte count. */
  def calculate(algorithm: String, data: Iterator[Bytes]): (Array[Byte], Long) = {
    val digest = MessageDigest getInstance algorithm
    val size = data.foldLeft(0L){ case (sum, bytes) =>
      digest update (bytes.data, bytes.offset, bytes.length)
      sum + bytes.length
    }
    (digest.digest, size)
  }

  /** @return Hash, byte count, and data processing result. */
  def calculate[T](algorithm: String, data: Iterator[Bytes], withData: Iterator[Bytes] => T): (Array[Byte], Long, T) = {
    val digester = MessageDigest getInstance algorithm
    var size = 0L
    val result = withData(data.map { bytes =>
      digester update (bytes.data, bytes.offset, bytes.length)
      size += bytes.length
      bytes
    })
    (digester.digest, size, result)
  }
}
