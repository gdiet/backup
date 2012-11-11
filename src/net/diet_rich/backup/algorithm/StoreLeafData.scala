package net.diet_rich.backup.algorithm

import net.diet_rich.util.io.{Reader,SeekReader}

trait StoreLeafData {
  def tree: BackupTree
  def filterPrint[ReturnType](input: Reader)(reader: Reader => ReturnType): (Long, ReturnType)
  def filterHash[ReturnType](input: Reader)(reader: Reader => ReturnType): (Array[Byte], ReturnType)
  def storeAndGetDataIdAndSize(reader: Reader): (Long, Long)
  def storeAndGetDataId(bytes: Array[Byte], size: Long): Long
  
  final def storeFromReader(src: SourceEntry, dst: Long, reader: SeekReader): Unit = {
    val (print, (hash, (dataid, size))) = filterPrint(reader) { reader =>
      filterHash(reader) { reader =>
        storeAndGetDataIdAndSize(reader)
      }
    }
    tree.setData(dst, Some(SimpleDataEntry(src.time, size, print, hash, dataid)))
  }

  final def storeFromBytesRead(src: SourceEntry, dst: Long, bytes: Array[Byte], print: Long, size: Long, hash: Array[Byte]): Unit = {
    val dataid = storeAndGetDataId(bytes, size)
    tree.setData(dst, Some(SimpleDataEntry(src.time, size, print, hash, dataid)))
  }

}