package net.diet_rich.sb

import java.security.MessageDigest
import java.io.InputStream
import java.security.DigestInputStream

object HashProvider {
  def digest(algorithm: String) : MessageDigest =
    MessageDigest.getInstance(algorithm)
 
  def filter(stream: InputStream, algorithm: String) : DigestInputStream =
    new DigestInputStream(stream, digest(algorithm))
}