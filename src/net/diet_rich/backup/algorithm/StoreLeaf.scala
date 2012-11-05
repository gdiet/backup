package net.diet_rich.backup.algorithm

import net.diet_rich.util.io.{SeekReader, Reader, readAndDiscardAll}

/** This implementation assumes that re-reading data from the source reader is fast.
 *  It is not optimized in terms of that the hash is calculated twice for some files
 *  although this could be avoided.
 */
trait StoreLeafNotOptimized {
  def tree: BackupTree
  def filterPrint[ReturnType](input: Reader)(reader: Reader => ReturnType): (Long, ReturnType)
  def filterHash[ReturnType](input: Reader)(reader: Reader => ReturnType): (Array[Byte], ReturnType)
  def calculatePrintAndReset(reader: SeekReader): Long
  
  final def storeLeaf(src: SourceEntry, dst: Long): Unit = src.read {reader =>
    storeLeaf(src, dst, reader, calculatePrintAndReset(reader))
  }
  
  final def storeLeaf(src: SourceEntry, dst: Long, reader: SeekReader, print: Long): Unit =
    if (tree.hasMatch(src.size, print))
      storeWithMatch(src, dst, reader)
    else
      storeWithoutMatch(src, dst, reader)

  private def storeWithMatch(src: SourceEntry, dst: Long, reader: SeekReader) {
    val (print, (hash, size)) = filterPrint(reader) { reader =>
      filterHash(reader) { reader =>
        readAndDiscardAll(reader)
      }
    }
    tree.dataid(size, print, hash) match {
      case None => storeWithoutMatch(src, dst, reader)
      case Some(dataid) => tree.setData(dst, Some(SimpleDataEntry(src.time, size, print, hash, dataid)))
    }
  }
  
  def storeWithoutMatch(src: SourceEntry, dst: Long, reader: SeekReader)
}


trait StoreLeaf {
  def tree: BackupTree
  def filterPrint[ReturnType](input: Reader)(reader: Reader => ReturnType): (Long, ReturnType)
  def filterHash[ReturnType](input: Reader)(reader: Reader => ReturnType): (Array[Byte], ReturnType)
  def calculatePrintAndReset(reader: SeekReader): Long
  
  final def storeLeaf(src: SourceEntry, dst: Long): Unit = src.read {reader =>
    storeLeaf(src, dst, reader, calculatePrintAndReset(reader))
  }
  
  final def storeLeaf(src: SourceEntry, dst: Long, reader: SeekReader, print: Long): Unit =
    if (tree.hasMatch(src.size, print))
      storeWithMatch(src, dst, reader)
    else
      storeWithoutMatch(src, dst, reader)

  private def storeWithMatch(src: SourceEntry, dst: Long, reader: SeekReader) {
    val (print, (hash, size)) = filterPrint(reader) { reader =>
      filterHash(reader) { reader =>
        readAndDiscardAll(reader)
      }
    }
    tree.dataid(size, print, hash) match {
      case None => storeWithoutMatch(src, dst, reader)
      case Some(dataid) => tree.setData(dst, Some(SimpleDataEntry(src.time, size, print, hash, dataid)))
    }
  }
  
  def storeWithoutMatch(src: SourceEntry, dst: Long, reader: SeekReader)
}