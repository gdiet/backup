// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values._

trait StoreSettings {
  def hashAlgorithm = "MD5"
  def threadPoolSize = 4
  def storeMethod = StoreMethod.DEFLATE
}

trait DataSettings {
  def blocksize: Size = Size(0x800000L)
}
