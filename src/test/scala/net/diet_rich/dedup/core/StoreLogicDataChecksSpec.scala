package net.diet_rich.dedup.core

import java.io.ByteArrayInputStream

import net.diet_rich.dedup.core.data.{Print, Bytes}
import net.diet_rich.dedup.core.meta.MetaBackend
import org.specs2.Specification

class StoreLogicDataChecksSpec extends Specification { def is = s2"""
${"Tests for the main part of the store logic".title}

If the size/print combination is not yet known, the data is immediately stored $sizePrintNotKnown
If the size/print combination is already known, data pre-loading is attempted $todo
The data is preloaded only if there is enough main memory available $todo
If the data has been preloaded,
  it is stored if the size/print/hash combination is not yet known $todo
  it is referenced if the size/print/hash combination is already known $todo
If preloaded data is stored, the memory is freed as the data is written $todo
If not enough memory is available for pre-loading,
  and the source is resettable, the data is pre-scanned before storing $todo
  and the source is not resettable, the data is stored immediately (even if it is a duplicate) $todo
If the data is pre-scanned before storing,
  and a matching size/print/hash combination is found, it is referenced $todo
  and no matching size/print/hash combination is found, the source is reset and stored $todo
Hash, size, print are stored correctly and the data is correctly packed
  when storing source data directly or with pre-calculated print $todo
  when storing source data with pre-calculaed size, print and hash $todo
Packed data is stored correctly with the correct data entries $todo
"""

  class MetaStub extends MetaBackend {
    def entry(id: Long) = ???
    def hasSizeAndPrint(size: Long, print: Long): Boolean = ???
    def markDeleted(id: Long, deletionTime: Option[Long]) = ???
    def createDataTableEntry(reservedID: Long, size: Long, print: Long, hash: Array[Byte], storeMethod: Int) = ???
    def dataEntry(dataid: Long) = ???
    def children(parent: Long) = ???
    def children(parent: Long, name: String) = ???
    def childrenWithDeleted(parent: Long) = ???
    def storeEntries(dataid: Long) = ???
    def createUnchecked(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]) = ???
    def nextDataID = ???
    def createByteStoreEntry(dataid: Long, start: Long, fin: Long) = ???
    def sizeOf(dataid: Long) = ???
    def change(id: Long, newParent: Long, newName: String, newTime: Option[Long], newData: Option[Long], newDeletionTime: Option[Long]) = ???
    def close() = ???
    def dataEntriesFor(size: Long, print: Long, hash: Array[Byte]) = ???
    def entries(path: String) = ???
    def create(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]) = ???
    def createOrReplace(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]) = ???
    def inTransaction[T](f: => T) = ???
    def createWithPath(path: String, changed: Option[Long], dataid: Option[Long]) = ???
  }

  class LogicStub(val metaBackend: MetaBackend, val storeMethod: Int = 1, val hashAlgorithm: String = "MD5") extends StoreLogicDataChecks {
    def storePackedData(data: Iterator[Bytes], estimatedSize: Long): Ranges = ???
  }

  def emptySource = Source.from(new ByteArrayInputStream(Array()), 0)

  def sizePrintNotKnown = {
    val meta = new MetaStub {
      override def hasSizeAndPrint(size: Long, print: Long) = {
        require(size  == 0L && print == Print.empty, s"size: $size, print: $print")
        false
      }
    }
    val logic = new LogicStub(meta) {
      override protected def storeSourceData(printData: Bytes, print: Long, data: Iterator[Bytes], estimatedSize: Long): Long = 42
    }
    logic.dataidFor(emptySource) === 42
  }

}
