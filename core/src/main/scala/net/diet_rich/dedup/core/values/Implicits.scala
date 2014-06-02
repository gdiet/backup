// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

import net.diet_rich.dedup.util.Bytes

object Implicits {
  implicit def treeEntryToID(entry: TreeEntry): TreeEntryID = entry.id
  implicit class EnrichedBytes(val bytes: Bytes) extends AnyVal {
    def size: Size = Size(bytes.length)
    def withOffset(offset: Size) = bytes.withOffset(offset.value.toInt)
  }
}
