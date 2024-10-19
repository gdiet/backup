package dedup

import scala.language.implicitConversions
import scala.util.ChainingOps

/** @see [[scala.util.ChainingSyntax]] */
@`inline` implicit def scalaUtilChainingOps[A](a: A): ChainingOps[A] = ChainingOps(a)

def now = Time(java.lang.System.currentTimeMillis)

/** Format a possibly large number of bytes, e.g. "27.38 MB" or "135 B". */
def readableBytes(l: Long): String =
  if      l < 10000          then "%d B"    .format(l)
  else if l < 1000000        then "%,.2f kB".format(l/1000d)
  else if l < 1000000000     then "%,.2f MB".format(l/1000000d)
  else if l < 1000000000000L then "%,.2f GB".format(l/1000000000d)
  else                            "%,.2f TB".format(l/1000000000000d)

class EnsureFailed(reason: String, cause: Throwable = null) extends IllegalArgumentException(reason, cause)

case object Failed
type Failed = Failed.type

def failure(failureMessage: String): Nothing =
  throw new EnsureFailed(failureMessage).tap(main.error(failureMessage, _))

def problem(marker: String, warningMessage: => String): Unit =
  ensure(marker, false, warningMessage)

def ensure(marker: String, condition: Boolean, warningMessage: => String): Unit =
  if !condition then
    main.error(s"$marker: $warningMessage")
    if !sys.props.isDefinedAt(s"suppress.$marker") then throw new EnsureFailed(s"$marker - $warningMessage")

/** Call this function before exiting to give logging some time to finish writing messages. */
def finishLogging(): Unit = Thread.sleep(200)
