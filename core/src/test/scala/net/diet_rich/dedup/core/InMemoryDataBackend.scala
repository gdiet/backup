// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values._

trait InMemoryDataBackend { _: FileSystemData =>
  import dataSettings.blocksize

  val disk = new Array[Byte](0x100000)

  def int(long: LongValue) = {
    assert (long.value <= Int.MaxValue)
    long.value.toInt
  }

  def writeData(data: Bytes, range: DataRange): Unit = {
    assert(range.size.value > 0, s"empty range $range")
    assert(range.start.value / blocksize.value == (range.fin.value - 1) / blocksize.value, s"range $range across block of size $blocksize")
    assert(range.size.value <= data.length, s"range $range is longer than the data length ${data.length}")
    System.arraycopy(data.data, data.offset, disk, int(range.start), int(range.size))
  }

  def readData(entry: StoreEntry): Iterator[Bytes] =
    Iterator(Bytes(disk, entry.range.start.value.toInt, entry.range.size.value.toInt))

}
