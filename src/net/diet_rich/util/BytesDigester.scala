package net.diet_rich.util

trait BytesDigester {
  def digest : Bytes
  def write(data: Bytes) : BytesDigester
}

object BytesDigester {
  def apply(algorithm: String) : BytesDigester = new BytesDigester {
    val digestObject = java.security.MessageDigest.getInstance(algorithm)
    def write(data: Bytes) : BytesDigester = {
      digestObject.update(data.data, data.intOffset, data.intSize)
      this
    }
    def digest : Bytes = Bytes(digestObject.digest)
  }
}