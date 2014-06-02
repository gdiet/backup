// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.util.Bytes

trait InMemoryDataBackend { _: FileSystemData =>
  import dataSettings.blocksize

  val disk = new Array[Byte](0x100000)

  def writeData(data: Bytes, offset: Position, range: DataRange): Unit = {
    assume(range.start.value / blocksize.value == (range.fin.value - 1) / blocksize.value, s"range $range across block of size $blocksize")
    // FIXME implementation missing
  }

}
