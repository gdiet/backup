// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values._

trait StoreSettingsSlice {
  trait StoreSettings {
    def hashAlgorithm: String
    def threadPoolSize: Int
    def storeMethod: StoreMethod
  }
  def storeSettings: StoreSettings
}
