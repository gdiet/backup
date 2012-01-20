// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import net.diet_rich.util.data.Bytes

object DataDefinitions {
  case class ParentAndName(parent: Long, name: String)
  case class IdAndName(id: Long, name: String)
  case class TimeAndData(time: Long, dataId: Long)
  /** Same as TimeAndData but different name to improve legibility in certain contexts. */
  type StoredFileInfo = TimeAndData
  case class SizePrint(size: Long, print: Long)
  case class TimeSize(time: Long, size: Long)
  case class TimeSizePrint(time: Long, size: Long, print: Long)
  case class TimeSizePrintHash(time: Long, size: Long, print: Long, hash: Bytes)
  case class FullFileData(time: Long, size: Long, print: Long, hash: Bytes, dataId: Long)
}