package net.diet_rich.backup.algorithm

import net.diet_rich.util.io.{SeekReader, Reader, readAndDiscardAll, fillFrom}

/** This implementation assumes that re-reading data from the source reader is fast.
 *  Also, it fully re-calculates the hash for large files that can't be cached,
 *  if a file with the same size and print is already stored.
 */
trait StoreLeaf {
  def tree: BackupTree
  def filterPrint[ReturnType](input: Reader)(reader: Reader => ReturnType): (Long, ReturnType)
  def filterHash[ReturnType](input: Reader)(reader: Reader => ReturnType): (Array[Byte], ReturnType)
  def calculatePrintAndReset(reader: SeekReader): Long
  def getLargeArray(size: Long): Option[Array[Byte]]
  def storeFromReader(src: SourceEntry, dst: Long, reader: SeekReader): Unit
  def storeFromBytesRead(src: SourceEntry, dst: Long, bytes: Array[Byte], print: Long, size: Long, hash: Array[Byte]): Unit
    
  final def storeLeaf(src: SourceEntry, dst: Long): Unit = src.read {reader =>
    storeLeaf(src, dst, reader, calculatePrintAndReset(reader))
  }
  
  final def storeLeaf(src: SourceEntry, dst: Long, reader: SeekReader, print: Long): Unit =
    if (tree.hasMatch(src.size, print))
      cacheWhileCalcuatingHash(src, dst, reader)
    else
      storeFromReader(src, dst, reader)

  private def cacheWhileCalcuatingHash(src: SourceEntry, dst: Long, reader: SeekReader): Unit = {
    val cacheSize = src.size + 1
    val cache = getLargeArray(cacheSize)
    val (print, (hash, size)) = filterPrint(reader) { reader =>
      filterHash(reader) { reader =>
        cache match {
          case None => readAndDiscardAll(reader)
          case Some(bytes) => fillFrom(reader, bytes, 0, bytes.length) + readAndDiscardAll(reader)
        }
      }
    }
    tree.dataid(size, print, hash) match {
      case Some(dataid) => tree.setData(dst, Some(SimpleDataEntry(src.time, size, print, hash, dataid)))
      case None => cache match {
        case Some(bytes) if size+1 == cacheSize =>
          storeFromBytesRead(src, dst, bytes, print, size, hash)
        case _ =>
          reader.seek(0)
          storeFromReader(src, dst, reader)
      }
    }
  }
  
}
