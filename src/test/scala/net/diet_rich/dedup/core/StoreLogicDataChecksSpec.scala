package net.diet_rich.dedup.core

import java.io.ByteArrayInputStream

import scala.collection.mutable
import scala.language.reflectiveCalls

import net.diet_rich.dedup.core.data.{Hash, Print, Bytes}
import net.diet_rich.dedup.core.meta.{DataEntry, MetaBackend}
import org.specs2.Specification

// FIXME try to use the specs 3.0 Spec trait instead
class StoreLogicDataChecksSpec extends Specification { def is = s2"""
${"Tests for the main part of the store logic".title}

If the size/print combination is not yet known, the data is immediately stored $sizePrintNotKnown
If the size/print combination is already known, data pre-loading is attempted $attemptPreload
The data is preloaded if there is enough main memory available $preloadIfMemoryAvailable
The data is not preloaded if there is not enough main memory available $dontPreloadIfMemoryNotAvailable
If the data has been preloaded,
  it is stored if the size/print/hash combination is not yet known $storeUnknownPreloaded
  it is referenced if the size/print/hash combination is already known $referenceKnownPreloaded
If preloaded data is stored, the memory is freed as the data is written $memoryFreedPreloaded
If not enough memory is available for pre-loading,
  and the source is resettable, the data is pre-scanned before storing $prescanResettableSource
  and the source is not resettable, the data is stored immediately (even if it is a duplicate) $todo
If the data is pre-scanned before storing,
  and a matching size/print/hash combination is found, it is referenced $todo
  and no matching size/print/hash combination is found, the source is reset and stored $todo
Hash, size, print are stored correctly and the data is correctly packed
  when storing source data directly or with pre-calculated print $todo
  when storing source data with pre-calculated size, print and hash $todo
Packed data is stored correctly with the correct data entries $todo

The store process itself should be tested $todo
"""

  class MetaStub extends MetaBackend {
    def entry(id: Long) = ???
    def hasSizeAndPrint(size: Long, print: Long): Boolean = ???
    def markDeleted(id: Long, deletionTime: Option[Long]) = ???
    def createDataTableEntry(reservedid: Long, size: Long, print: Long, hash: Array[Byte], storeMethod: Int) = ???
    def dataEntry(dataid: Long) = ???
    def children(parent: Long) = ???
    def children(parent: Long, name: String) = ???
    def childrenWithDeleted(parent: Long) = ???
    def storeEntries(dataid: Long) = ???
    def createUnchecked(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]) = ???
    def nextDataid = ???
    def createByteStoreEntry(dataid: Long, start: Long, fin: Long) = ???
    def sizeOf(dataid: Long) = ???
    def change(id: Long, newParent: Long, newName: String, newTime: Option[Long], newData: Option[Long], newDeletionTime: Option[Long]) = ???
    def close() = ???
    def dataEntriesFor(size: Long, print: Long, hash: Array[Byte]): List[DataEntry] = ???
    def entries(path: String) = ???
    def create(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]) = ???
    def createOrReplace(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]) = ???
    def inTransaction[T](f: => T) = ???
    def createWithPath(path: String, changed: Option[Long], dataid: Option[Long]) = ???
    def settings = ???
    def replaceSettings(newSettings: Map[String, String]) = ???
  }

  class LogicStub(val metaBackend: MetaBackend = new MetaStub, val storeMethod: Int = 1, val hashAlgorithm: String = "MD5") extends StoreLogicDataChecks {
    def storePackedData(data: Iterator[Bytes], estimatedSize: Long): Ranges = ???
  }

  class SourceStub extends Source {
    def size: Long = ???
    def read(count: Int): Bytes = ???
    def close(): Unit = ???
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

  def attemptPreload = {
    val meta = new MetaStub {
      override def hasSizeAndPrint(size: Long, print: Long) = {
        require(size  == 0L && print == Print.empty, s"size: $size, print: $print")
        true
      }
    }
    val logic = new LogicStub(meta) {
      override def tryPreloadDataThatMayBeAlreadyKnown(printData: Bytes, print: Long, source: Source): Long = 43
    }
    logic.dataidFor(emptySource) === 43
  }

  def preloadIfMemoryAvailable = {
    val logic = new LogicStub() {
      val _tryPreloadDataThatMayBeAlreadyKnown = tryPreloadDataThatMayBeAlreadyKnown _
      override def preloadDataThatMayBeAlreadyKnown(printData: Bytes, print: Long, source: Source): Long = 44
    }
    logic._tryPreloadDataThatMayBeAlreadyKnown(Bytes.empty, 0L, emptySource) === 44
  }

  def dontPreloadIfMemoryNotAvailable = {
    val source = new SourceStub { override def size = Long.MaxValue ; override def read(count: Int) = Bytes.empty }
    val logic = new LogicStub() {
      val _tryPreloadDataThatMayBeAlreadyKnown = tryPreloadDataThatMayBeAlreadyKnown _
      override def storeSourceData(printData: Bytes, print: Long, data: Iterator[Bytes], estimatedSize: Long): Long = 45
    }
    logic._tryPreloadDataThatMayBeAlreadyKnown(Bytes.empty, 0L, source) === 45
  }

  def storeUnknownPreloaded = {
    val meta = new MetaStub {
      override def dataEntriesFor(size: Long, print: Long, hash: Array[Byte]): List[DataEntry] = {
        require(size  == 0L && print == 1234, s"size: $size, print: $print")
        require(hash.deep == Hash.empty("MD5").deep)
        Nil
      }
    }
    val logic = new LogicStub(meta) {
      val _preloadDataThatMayBeAlreadyKnown = preloadDataThatMayBeAlreadyKnown _
      override def storeSourceData(data: Iterator[Bytes], size: Long, print: Long, hash: Array[Byte]): Long = 46
    }
    logic._preloadDataThatMayBeAlreadyKnown(Bytes.empty, 1234, emptySource) === 46
  }

  def referenceKnownPreloaded = {
    val result = DataEntry(47,1,2,Array(),3)
    val meta = new MetaStub {
      override def dataEntriesFor(size: Long, print: Long, hash: Array[Byte]): List[DataEntry] = {
        require(size  == 0L && print == 1234, s"size: $size, print: $print")
        require(hash.deep == Hash.empty("MD5").deep)
        List(result)
      }
    }
    val logic = new LogicStub(meta) {
      val _preloadDataThatMayBeAlreadyKnown = preloadDataThatMayBeAlreadyKnown _
    }
    logic._preloadDataThatMayBeAlreadyKnown(Bytes.empty, 1234, emptySource) === 47
  }

  // FIXME try org.specs.specification.core.Env, maybe like
  /*
  class MySpec extends Specification with CommandLineArguments { def is(args: CommandLine) = s2"""
    Do something here with a command line parameter ${args.valueOr("parameter1", "not found")}
  """
  see http://etorreborre.blogspot.de/
  maybe not before specs 3.0
  }
   */
  def memoryFreedPreloaded: org.specs2.execute.Result = if (!sys.env.contains("tests.include.longRunning")) skipped("- to include this test, set tests.include.longRunning") else {
    val memoryForTest = 300000000
    def freeMemory = Runtime.getRuntime.maxMemory() - (Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory())
    require(freeMemory > memoryForTest, s"required free memory $memoryForTest not availabe. Free memory is $freeMemory")
    def source = new SourceStub {
      var count = 0
      override val size: Long = memoryForTest
      override def read(size: Int) = {
        val currentSize = math.min(size, memoryForTest - count)
        count += currentSize
        Bytes.zero(currentSize)
      }
    }
    val expectedHash = Hash.calculate("MD5", source.allData)._1
    val meta = new MetaStub {
      override def dataEntriesFor(size: Long, print: Long, hash: Array[Byte]): List[DataEntry] = {
        require(size  == memoryForTest && print == 1234, s"size: $size, print: $print")
        require(hash.deep == expectedHash.deep)
        Nil
      }
    }
    val logic = new LogicStub(meta) {
      var memoryProtocol = List[Long]()
      val _preloadDataThatMayBeAlreadyKnown = preloadDataThatMayBeAlreadyKnown _
      override def storeSourceData(data: Iterator[Bytes], size: Long, print: Long, hash: Array[Byte]): Long = {
        data.grouped(3000).map { _ =>
          Runtime.getRuntime.gc()
          memoryProtocol = memoryProtocol :+ freeMemory
        }.toList
        48
      }
    }
    val result = logic._preloadDataThatMayBeAlreadyKnown(Bytes.empty, 1234, source)
    val memoryFreed = logic.memoryProtocol.sliding(2,1).map{case (a::b::Nil) => b - a; case _ => ???}
    (result === 48) and
      (memoryFreed should haveSize(3)) and
      (memoryFreed should contain(be_>( 90000000L)).foreach) and
      (memoryFreed should contain(be_<(110000000L)).foreach)
  }

  def prescanResettableSource = {
    val sourceSize = Runtime.getRuntime.maxMemory() * 2
    val sourceBytes = Bytes(Array(50.toByte), 0, 1)
    def source = new SourceStub with ResettableSource {
      var firstCall = true
      override val size: Long = sourceSize
      override def read(size: Int) = if (firstCall) { firstCall = false; sourceBytes } else Bytes.empty
      def reset = Unit
    }
    val expectedHash = Hash.calculate("MD5", source.allData)._1
    val meta = new MetaStub {
      override def dataEntriesFor(size: Long, print: Long, hash: Array[Byte]): List[DataEntry] = {
        require(size  == 1 && print == 4321, s"size: $size, print: $print")
        require(hash.deep == expectedHash.deep)
        Nil
      }
    }
    val logic = new LogicStub(meta) {
      val _tryPreloadDataThatMayBeAlreadyKnown = tryPreloadDataThatMayBeAlreadyKnown _
      override def storeSourceData(source: Source): Long = 49
    }
    logic._tryPreloadDataThatMayBeAlreadyKnown(Bytes.empty, 4321, source) === 49
  }
}
