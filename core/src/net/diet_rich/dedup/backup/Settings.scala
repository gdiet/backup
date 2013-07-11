// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.util.vals.Size
import net.diet_rich.util.vals.Bytes

trait Settings {
  val checkReferencePrints: Boolean
  val backend: Backend
  val hashAlgorithm: String
  val smallCacheLimit: Size
  val cacheLimit: Size
  def cache(size: Size): Seq[Bytes]
  def largeCacheExecution[T](f: => T): T
}
