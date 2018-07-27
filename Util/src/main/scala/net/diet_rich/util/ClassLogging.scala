package net.diet_rich.util

import org.slf4j.{Logger, LoggerFactory}

trait ClassLogging {
  protected val log: Logger = LoggerFactory.getLogger(getClass)
}
