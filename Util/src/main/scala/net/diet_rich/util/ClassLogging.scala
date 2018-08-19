package net.diet_rich.util

import org.slf4j.{Logger, LoggerFactory}

trait ClassLogging {
  protected val log: Logger = LoggerFactory.getLogger(getClass)
  protected def log[T](message: => String)(f: => T): T =
    if (!log.isDebugEnabled) f
    else try init(f)(t => log.debug(s"$message -> $t")) catch {
      case e: Throwable => log.error(s"$message -> $e", e); throw e
    }
}
