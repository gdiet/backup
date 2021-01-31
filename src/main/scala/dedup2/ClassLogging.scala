package dedup2

import org.slf4j.{Logger, LoggerFactory}

trait ClassLogging {
  protected def trace_(msg: => String): Unit = if (log.isTraceEnabled) log.trace(msg)
  protected def info_ (msg: => String): Unit = if (log.isInfoEnabled)  log.info (msg)
  protected def warn_ (msg: => String): Unit = if (log.isWarnEnabled)  log.warn (msg)
  protected def error_(msg: String)              : Unit = log.error(msg)
  protected def error_(msg: String, e: Throwable): Unit = log.error(msg, e)
  private val log: Logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))
}
