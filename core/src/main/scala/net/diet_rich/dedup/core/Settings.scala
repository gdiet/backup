// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values.StoreMethod

trait StoreSettingsSlice {
  def storeSettings: StoreSettings
}

final case class StoreSettings (hashAlgorithm: String, threadPoolSize: Int, storeMethod: StoreMethod)
