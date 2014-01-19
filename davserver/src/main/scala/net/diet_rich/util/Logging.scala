// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import org.slf4j._

trait CallLogging {
  protected val logger = LoggerFactory getLogger (this getClass)
  
  private def log[R](isEnabled: => Boolean, log: String => Unit, logEx: (String, Throwable) => Unit)(message: => String)(code: => R): R =
    if (!isEnabled) code else {
      val msg = message
      try {  
        log(s"$msg >>")
        init(code){r => log(s"$msg << r")}
      } catch { case e: Exception =>
        logEx(s"$msg ##", e)
        throw e
      }
    }

  protected def error[R] = log[R](logger isErrorEnabled, logger error, logger error) _
  protected def warn[R]  = log[R](logger isWarnEnabled,  logger warn , logger warn ) _
  protected def info[R]  = log[R](logger isInfoEnabled,  logger info , logger info ) _
  protected def debug[R] = log[R](logger isDebugEnabled, logger debug, logger debug) _
  protected def trace[R] = log[R](logger isTraceEnabled, logger trace, logger trace) _
}

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
    
    def error(message: => String, e: Throwable) = if (logger isErrorEnabled) logger error (message, e)
    def warn (message: => String, e: Throwable) = if (logger isWarnEnabled)  logger warn  (message, e)
    def info (message: => String, e: Throwable) = if (logger isInfoEnabled)  logger info  (message, e)
    def debug(message: => String, e: Throwable) = if (logger isDebugEnabled) logger debug (message, e)
    def trace(message: => String, e: Throwable) = if (logger isTraceEnabled) logger trace (message, e)
  }
}
