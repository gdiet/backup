package util

import java.security.MessageDigest
import scala.util.chaining._

object Hash {
  def apply[T](algorithm: String, bytes: Seq[Array[Byte]])(f: Seq[Array[Byte]] => T): (Array[Byte], T) = {
    val md = MessageDigest.getInstance(algorithm)
    val t = f(bytes.map(_.tap(md.update)))
    md.digest -> t
  }
}
