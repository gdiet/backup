package dedup

import scala.language.implicitConversions
import scala.util.ChainingOps

/** @see [[scala.util.ChainingSyntax]] */
@`inline` implicit final def scalaUtilChainingOps[A](a: A): ChainingOps[A] = ChainingOps(a)

def now = Time(java.lang.System.currentTimeMillis)

/** Format a possibly large number of bytes, e.g. "27.38 MB" or "135 B". */
def readableBytes(l: Long): String =
  if      l < 10000          then "%d B"    .format(l)
  else if l < 1000000        then "%,.2f kB".format(l/1000d)
  else if l < 1000000000     then "%,.2f MB".format(l/1000000d)
  else if l < 1000000000000L then "%,.2f GB".format(l/1000000000d)
  else                            "%,.2f TB".format(l/1000000000000d)
