// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values._

trait DataBackendInterface {
  def read(entry: DataRange): Iterator[Bytes]
  def write(data: Bytes, start: Position): Unit
}
