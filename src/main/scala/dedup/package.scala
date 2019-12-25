import java.util.concurrent.atomic.AtomicBoolean

import org.slf4j.Logger

import scala.util.ChainingSyntax

package object dedup extends ChainingSyntax {
  val copyWhenMoving = new AtomicBoolean(false)

  def assumeLogged(condition: Boolean, message: => String)(implicit log: Logger): Unit =
    if (!condition) log.error(s"Assumption failed: $message", new IllegalStateException(""))

  def assertLogged(condition: Boolean, message: => String)(implicit log: Logger): Unit =
    if (!condition) throw new IllegalStateException(message).tap(log.error(s"Assumption failed: $message", _))

  type StartStop = Option[(Long, Long)]
  implicit class StartStopDecorator(val startStop: StartStop) {
    def size: Long = startStop.map { case (a,b) => b - a }.getOrElse(0)
    def start: Long = startStop.map(_._1).getOrElse(0)
    def stop: Long = startStop.map(_._1).getOrElse(0)
  }
}
