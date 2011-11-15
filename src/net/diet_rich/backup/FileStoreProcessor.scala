// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import akka.dispatch.{Future,Futures}
import java.io.File
import net.diet_rich.scala.ManagedIO._
import net.diet_rich.util.Bytes
import net.diet_rich.util.data.{Checksum,Digester}
import net.diet_rich.util.io.RandomAccessFileInput
import net.diet_rich.util.logging.Logged

object FileStoreProcessor extends Logged {

  private val dataChunkSize : Int = BackupSystemConfig()("processing.ChunkSize", 524288)    // 512 kB
  private val maxChunkSize : Int = BackupSystemConfig()("processing.MaxChunkSize", 3145728) //   3 MB
  assert (maxChunkSize >= dataChunkSize)

  // FIXME make a repository settings
  private val headerChunkSize : Int = 1024
  private def newHeaderDigester() = Digester.crcadler()
  private def newHashDigester() = Digester.hash("MD5")
  assert (maxChunkSize >= headerChunkSize)
  
  // FIXME implement storage database (two tables: id/header/hash/sourceSize and id/index/position/storageSize
  case class StoreEntry(id : Long)
  case class StoredPiece(position: Long, size: Long)
  trait StoreMethod
  object Uncompressed extends StoreMethod
  object Deflated extends StoreMethod
  case class StoredData(method: StoreMethod, pieces : List[StoredPiece])
  def dbContains(size: Long, headerChecksum: Checksum) : Boolean = false
  def dbLookup(size: Long, headerChecksum: Checksum, hash: Bytes) : Option[StoreEntry] = None
  def dbCreateEntry(size: Long, headerChecksum: Checksum, hash: Bytes) : StoreEntry = StoreEntry(0)
  def dbDeleteEntry(entry: StoreEntry) = Unit
  def dbCompleteEntry(entry: StoreEntry, size: Long, headerChecksum: Checksum, hash: Bytes, storedData: StoredData) : StoreEntry = StoreEntry(0)
  def dbTransaction[T](task: => T) = task

  // FIXME implement storage
  /** @throws Exception on error state in storage. */
  def store(data: Stream[Bytes]) : StoredData = StoredData(Uncompressed, Nil)

  
  // Note: May not be called at all for files that have the same
  // signature as during a previous backup.
  // Note: When updating a previous backup, it is a good idea to
  // call directly FIXME storeProbablyKnown or storeProbablyNew based
  // on e.g. whether the modification time stamp matches as well.
  def linkOrStore(file: File, headerChunkSize: Int) = {
    
    // FIXME exception handling
    val storeEntry = source(file).produce(headerProcessor(file.length()))
    // FIXME continue

  }

  def source(file: File) = PushingSource[Bytes](
    new DataSource[Bytes] {
      debug("creating source for file", file)
      
      // read completely to memory if not too large
      val firstReadSize =
        if (input.length <= maxChunkSize) input.length.toInt + 1
        else math.max(headerChunkSize, dataChunkSize)
        
      val input = new RandomAccessFileInput(file)
      var firstRead = true;
      
      override def close = input.close()
      
      override def fetch : SourceSignal[Bytes] = {
        val bytes = Bytes(
            if (firstRead) { firstRead = false ; firstReadSize }
            else dataChunkSize
          )
        input.readFully(bytes) match {
          case bytes if bytes.length == 0 => EOI()
          case bytes                      => Next(bytes)
        }
      }
      
    }
  )
  
  def headerProcessor(probableSize: Long) : InlineDataProcessor[Bytes,StoreEntry] = {
    case EOI() =>
      debug("found 0-byte source file")
      Finished(StoreEntry(0)) // FIXME get or create real entry
    case Next(data) => {
      if (!data.filled) Finished(linkOrStoreFromMemory(data))
      else {
        val header = newHeaderDigester().write(data.keepAtMostFirst(headerChunkSize)).getDigest
        if (dbContains(probableSize, header)) {
          storeProbablyKnown
        } else {
          Finished(StoreEntry(0)) // FIXME
        }
      }
    }
    case SourceError(_) => ContinueSame()
  }

  def linkOrStoreFromMemory(data: Bytes) : StoreEntry = {
    debug("loaded complete source file into memory")
    val header = newHeaderDigester().write(data.keepAtMostFirst(headerChunkSize)).getDigest
    val hash = newHashDigester().write(data).getDigest
    dbTransaction {
      dbLookup(data.length, header, hash) match {
        case Some(entry) => (true, entry)
        case None => (false, dbCreateEntry(data.length, header, hash))
      }
    } match {
      case (true, entry) =>
        entry
      case (false, entry) =>
        val storedData = storeOrDeleteOnError(entry, Stream(data))
        dbCompleteEntry(entry, data.length, header, hash, storedData)
    }
  }

  def storeOrDeleteOnError(entry: StoreEntry, data: Stream[Bytes]) : StoredData = {
    try {
      store(data)
    } catch {
      case e =>
        dbDeleteEntry(entry)
        throw e
    }
  }
  
  def storeProbablyKnown : ProcessorSignal[Bytes,StoreEntry] = Finished(StoreEntry(0)) // FIXME
  
  def storeProbablyNew : ProcessorSignal[Bytes,StoreEntry] = Finished(StoreEntry(0)) // FIXME
  
}

//    def combinedStatus(one: PossibleProblem, two: PossibleProblem) : PossibleProblem = {
//      val futures = List(one, two)
//      Future{Futures.firstCompletedOf(futures).get
//        .orElse(futures.find(_.get.isDefined).get.get)}
//    }
    
//    def checkProblems(problems: List[PossibleProblem]) = {
//      problems.foldLeft((List[PossibleProblem](),List[Throwable]()))(
//          (lists, item) => {
//            if (item.isCompleted) {
//              (lists._1, lists._2 ++ item.get)
//            } else {
//              (item :: lists._1, lists._2)
//            }
//          })
//    }

//    def initialProcessor(status: PossibleProblem) : InlineDataProcessor[Bytes,Option[Throwable]] = {
//      case Right(EOI)            => finished(None)
//      case Right(SourceError(e)) => finished(Some(e))
//      case Left(bytes)           =>
//        continue(storeNowProcessor(status))
//    }
//    
//    
//    def storeNowProcessor(status: PossibleProblem) : InlineDataProcessor[Bytes,Option[Throwable]] = {
//      case Right(EOI)            => finished(None)
//      case Right(SourceError(e)) => finished(Some(e))
//      case Left(bytes)           =>
//        continue(storeNowProcessor(status))
//    }
