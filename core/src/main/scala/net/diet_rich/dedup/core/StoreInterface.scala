// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values.{TreeEntryID, Bytes, DataEntryID, Time}

trait StoreInterface {
  def read(entry: DataEntryID): Iterator[Bytes]
  def storeUnchecked(parent: TreeEntryID, name: String, source: Source, time: Time): TreeEntryID
}
