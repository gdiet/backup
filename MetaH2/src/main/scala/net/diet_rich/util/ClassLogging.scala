package net.diet_rich.util
import org.slf4j.{Logger, LoggerFactory}

trait ClassLogging {
  protected val log: Logger = LoggerFactory.getLogger(getClass)
  protected def log[T](message: => String, asTrace: Boolean = false)(f: => T): T =
    if (asTrace) log(message, log.isTraceEnabled, log.trace)(f)
    else log(message, log.isDebugEnabled, log.debug)(f)
  private def log[T](message: => String, enabled: Boolean, logFunction: String => Unit)(f: => T): T =
    if (!enabled) f
    else try init(f)(t => logFunction(s"$message -> $t")) catch {
      case e: Throwable => log.error(s"$message -> $e", e); throw e
    }
}
