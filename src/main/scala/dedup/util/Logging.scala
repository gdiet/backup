package dedup
package util

/** Provides logging methods. For optimum performance uses by-name string parameters.
  *
  * Instantiation needs e.g. 30 ns. When used in a class that might be instantiated million times per second,
  * consider using the companion object for class logging. In this case see also
  *
  * @see https://stackoverflow.com/questions/13713557/scala-accessing-protected-field-of-companion-objects-trait */
trait ClassLogging:

  protected val log: Slf4jLogger = Slf4jLogger(getClass.getName)

  protected def watch[T](msg: => String, logger: (=> String) => Unit = log.trace)(f: => T): T =
    logger(s">> $msg")
    val start = System.nanoTime()
    def time =
      val t = System.nanoTime() - start
      if      t         < 5000 then s"${t}ns"
      else if t/1000    < 5000 then s"${t/1000}µs"
      else if t/1000000 < 5000 then s"${t/1000000}ms"
      else                          s"${t/1000000000}s"
    try f.tap(t => logger(s"<< $time $msg -> $t"))
    catch { case t: Throwable => log.error(s"<< $time $msg -> ERROR", t); throw t }

  protected def guard[T](msg: => String)(f: => T): T =
    try f catch { case t: Throwable => log.error(s"$msg -> ERROR", t); throw t }


class Slf4jLogger(name: String):

  private val logger: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(name.stripSuffix("$"))
  def trace(msg: => String): Unit = if (logger.isTraceEnabled) logger.trace(msg)
  def debug(msg: => String): Unit = if (logger.isDebugEnabled) logger.debug(msg)
  def info (msg: => String): Unit = if (logger.isInfoEnabled)  logger.info (msg)
  def warn (msg: => String): Unit = if (logger.isWarnEnabled)  logger.warn (msg)
  def error(msg: => String): Unit = logger.error(msg)
  def trace(msg: => String, e: => Throwable): Unit = logger.trace(msg, e)
  def debug(msg: => String, e: => Throwable): Unit = logger.debug(msg, e)
  def info (msg: => String, e: => Throwable): Unit = logger.info(msg, e)
  def warn (msg: => String, e: => Throwable): Unit = logger.warn(msg, e)
  def error(msg: => String, e: => Throwable): Unit = logger.error(msg, e)
