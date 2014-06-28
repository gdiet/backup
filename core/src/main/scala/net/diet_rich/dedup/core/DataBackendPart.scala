// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values._

trait DataBackendPart {
  protected val dataBackend: DataBackend

  trait DataBackend {
    def read(entry: StoreEntry): Iterator[Bytes] // FIXME DataRange
    def write(data: Bytes, range: DataRange): Unit
  }
}
