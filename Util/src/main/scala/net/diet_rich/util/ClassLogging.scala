package net.diet_rich.util

import org.slf4j.{Logger, LoggerFactory}

trait ClassLogging {
  protected val log: Logger = LoggerFactory.getLogger(getClass)
  protected def log[T](message: => String)(f: => T): T =
    if (log.isDebugEnabled) init(f)(t => log.debug(s"$message -> $t"))
    else f
}
