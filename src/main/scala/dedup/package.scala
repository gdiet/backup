import java.util.concurrent.atomic.AtomicBoolean

import org.slf4j.Logger

import scala.util.ChainingSyntax

package object dedup extends ChainingSyntax {
  // https://stackoverflow.com/questions/58506337/java-byte-array-of-1-mb-or-more-takes-up-twice-the-ram
  val memChunk = 524200 // a bit less than 0.5 MiB to avoid problems with humongous objects in G1GC

  val copyWhenMoving = new AtomicBoolean(false)

  def assumeLogged(condition: Boolean, message: => String)(implicit log: Logger): Unit =
    if (!condition) log.error(s"Assumption failed: $message", new IllegalStateException(""))

  def assertLogged(condition: Boolean, message: => String)(implicit log: Logger): Unit =
    if (!condition) throw new IllegalStateException(message).tap(log.error(s"Assumption failed: $message", _))

  // FIXME old
  type StartStop = Option[(Long, Long)]
  implicit class StartStopDecorator(val startStop: StartStop) {
    def size: Long = startStop.map { case (a,b) => b - a }.getOrElse(0)
    def start: Long = startStop.map(_._1).getOrElse(0)
    def stop: Long = startStop.map(_._1).getOrElse(0)
  }
}
