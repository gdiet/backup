// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.util.vals.Size
import net.diet_rich.util.vals.Bytes

trait System extends System.ForStoreStrategySelect{
  val hashAlgorithm: String // ok
  val smallCacheLimit: Size // OK
  def cacheSource: Seq[Bytes] // ok, implement e.g. as Stream
  def largeCacheExecution[T](f: => T): T // OK
}
object System {
  trait ForStoreStrategySelect {
    val checkReferencePrints: Boolean // OK
    val backend: Backend // OK
  }
}
