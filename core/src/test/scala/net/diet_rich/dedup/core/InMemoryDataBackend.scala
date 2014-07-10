// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values.{DataRange, Position, LongValue, Bytes}

trait InMemoryDataBackendPart extends DataBackendSlice {
  object dataBackend extends DataBackend {
    val disk = new Array[Byte](0x100000)

    def int(long: LongValue) = {
      assert (long.value <= Int.MaxValue)
      long.value.toInt
    }

    override def write(data: Bytes, start: Position): Unit = {
      assert(data.length > 0, s"empty data")
      System.arraycopy(data.data, data.offset, disk, int(start), data length)
    }

    override def read(range: DataRange): Iterator[Bytes] =
      Iterator(Bytes(disk, range.start.value.toInt, range.size.value.toInt))
  }
}
