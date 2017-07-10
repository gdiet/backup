package net.diet_rich.common

import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.NonFatal

trait Logging {
  protected val log = new Logging.LogAdapter(LoggerFactory getLogger this.getClass)
}

object Logging {
  protected class LogAdapter (private val logger: Logger) {
    def error(message: => String): Unit = if (logger.isErrorEnabled) logger error message
    def warn (message: => String): Unit = if (logger.isWarnEnabled ) logger warn  message
    def info (message: => String): Unit = if (logger.isInfoEnabled ) logger info  message
    def debug(message: => String): Unit = if (logger.isDebugEnabled) logger debug message
    def trace(message: => String): Unit = if (logger.isTraceEnabled) logger trace message

    def error(message: => String, e: Throwable): Unit = if (logger.isErrorEnabled) logger error (message, e)
    def warn (message: => String, e: Throwable): Unit = if (logger.isWarnEnabled)  logger warn  (message, e)
    def info (message: => String, e: Throwable): Unit = if (logger.isInfoEnabled)  logger info  (message, e)
    def debug(message: => String, e: Throwable): Unit = if (logger.isDebugEnabled) logger debug (message, e)
    def trace(message: => String, e: Throwable): Unit = if (logger.isTraceEnabled) logger trace (message, e)

    def call[R](message: => String)(code: => R): R = {
      if (!logger.isDebugEnabled) code else {
        val msg = message
        try {
          debug(s"$msg >>")
          init(code){r => debug(s"$msg << $r")}
        } catch { case e: Throwable =>
          if (logger.isDebugEnabled) debug(s"$msg ##", e)
          else info(s"$msg has thrown an exception", e)
          throw e
        }
      }
    }
  }
}
