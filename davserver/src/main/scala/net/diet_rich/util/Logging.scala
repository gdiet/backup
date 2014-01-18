// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import org.slf4j._

trait Logging {
  protected val log = new Logging.LogAdapter(LoggerFactory getLogger (this getClass))
}

object Logging {
  class LogAdapter(private val logger: Logger) {
    def error(message: => String) = if (logger isErrorEnabled) logger error message
    def warn (message: => String) = if (logger isWarnEnabled ) logger warn  message
    def info (message: => String) = if (logger isInfoEnabled ) logger info  message
    def debug(message: => String) = if (logger isDebugEnabled) logger debug message
    def trace(message: => String) = if (logger isTraceEnabled) logger trace message
  }
}
