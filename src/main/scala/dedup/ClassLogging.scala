package dedup

import org.slf4j.{Logger, LoggerFactory}

/** Instantiation needs 20-50 ns. When used in a class that might be instantiated million times per second,
  * consider using the companion object for class logging, e.g. like this:
  *
  * {{{
  * object CompanionObject extends ClassLogging {
  *   @inline override protected def trace_(msg: => String): Unit = super.trace_(msg)
  *   @inline override protected def debug_(msg: => String): Unit = super.debug_(msg)
  *   @inline override protected def info_ (msg: => String): Unit = super.info_ (msg)
  *   @inline override protected def warn_ (msg: => String): Unit = super.warn_ (msg)
  *   @inline override protected def error_(msg:    String): Unit = super.error_(msg)
  *   @inline override protected def error_(msg: String, e: Throwable): Unit = super.error_(msg, e)
  * }
  * }}}
  *
  * @see https://stackoverflow.com/questions/13713557/scala-accessing-protected-field-of-companion-objects-trait
  */
trait ClassLogging {
  protected def trace_(msg: => String): Unit = if (log.isTraceEnabled) log.trace(msg)
  protected def debug_(msg: => String): Unit = if (log.isDebugEnabled) log.debug(msg)
  protected def info_ (msg: => String): Unit = if (log.isInfoEnabled)  log.info (msg)
  protected def warn_ (msg: => String): Unit = if (log.isWarnEnabled)  log.warn (msg)
  protected def error_(msg: String)              : Unit = log.error(msg)
  protected def error_(msg: String, e: Throwable): Unit = log.error(msg, e)
  private val log: Logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))
}
