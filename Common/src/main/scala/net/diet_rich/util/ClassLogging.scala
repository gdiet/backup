package net.diet_rich.util

import org.slf4j.LoggerFactory

trait ClassLogging {
  val log = LoggerFactory.getLogger(getClass)
}
