package net.diet_rich.dedup

import scala.util.control.NonFatal

package object core {
  val repositoryidKey = "repository id"
  val metaDir = "meta"
  val dataDir = "data"

  // FIXME use logger
  def warn(message: => Any) = println(s"WARN: $message")
  def warn(message: => Any, e: Throwable) = {
    println(s"WARN: $message")
    e.printStackTrace(System.out)
  }
  def suppressExceptions(code: => Unit) = try { code } catch { case NonFatal(e) => warn(s"an exception has occurred that is suppressed", e) }

  type StartFin = (Long, Long)
  type Ranges = Vector[StartFin]
  val RangesNil = Vector[StartFin]()
}
