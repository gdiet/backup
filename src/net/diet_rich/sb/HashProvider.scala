package net.diet_rich.sb

import java.security.MessageDigest
import java.io.InputStream
import java.security.DigestInputStream
import net.diet_rich.util.Configuration._

object HashProvider {
  def digester(algorithm: String) : MessageDigest =
    MessageDigest.getInstance(algorithm)

  def digester(repoSettings: StringMap) : MessageDigest =
    digester(repoSettings string "hash algorithm")
    
  def filter(stream: InputStream, algorithm: String) : DigestInputStream =
    new DigestInputStream(stream, digester(algorithm))
}