package dedup

import org.slf4j.{Logger, LoggerFactory}

/** Provides logging methods. For optimum performance uses by-name string parameters.
  *
  * Instantiation needs a couple of ns. When used in a class that might be instantiated million times per second,
  * consider using the companion object for class logging. In this case see also
  *
  * @see https://stackoverflow.com/questions/13713557/scala-accessing-protected-field-of-companion-objects-trait
  */
trait ClassLogging {
  protected object log {
    private val logger: Logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))
    def trace(msg: => String): Unit = if (logger.isTraceEnabled) logger.trace(msg)
    def debug(msg: => String): Unit = if (logger.isDebugEnabled) logger.debug(msg)
    def info (msg: => String): Unit = if (logger.isInfoEnabled)  logger.info (msg)
    def warn (msg: => String): Unit = if (logger.isWarnEnabled)  logger.warn (msg)
    def error(msg: String)              : Unit = logger.error(msg)
    def error(msg: String, e: Throwable): Unit = logger.error(msg, e)
  }
}
