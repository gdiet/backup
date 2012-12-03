package net.diet_rich.backup.alg

import net.diet_rich.util.io.Reader

trait ByteStoreDB {
  def storeAndGetDataId(bytes: Array[Byte], size: Long): Long
  def storeAndGetDataIdAndSize(reader: Reader): (Long, Long)
}
