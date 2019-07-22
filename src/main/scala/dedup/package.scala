import java.io.File

package object dedup extends scala.util.ChainingSyntax {
  def now: Long = System.currentTimeMillis
  def sync[T](f: => T): T = synchronized(f)

  implicit class RichOptions(val options: Map[String, String]) extends AnyVal {
    def fileFor(key: String): File = new File(options.getOrElse("repo", "")).getAbsoluteFile
  }
}
