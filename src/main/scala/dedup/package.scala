import org.slf4j.Logger

import scala.util.ChainingSyntax

package object dedup extends ChainingSyntax {
  def assumeLogged(condition: Boolean, message: => String)(implicit log: Logger): Unit =
    if (!condition) log.error(s"Assumption failed: $message", new IllegalStateException(""))
}
