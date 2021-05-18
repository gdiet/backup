package dedup

import scala.language.implicitConversions
import scala.util.ChainingOps

/** @see [[scala.util.ChainingSyntax]] */
@`inline` implicit final def scalaUtilChainingOps[A](a: A): ChainingOps[A] = ChainingOps(a)

def now = java.lang.System.currentTimeMillis
