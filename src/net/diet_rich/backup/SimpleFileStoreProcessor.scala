// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import akka.dispatch.{Future,Futures}
import java.io.File
import net.diet_rich.util.Bytes
import net.diet_rich.util.data.{Checksum,Digester}
import net.diet_rich.util.io.{Closeable,OutputStream,RandomAccessFileInput}

class SimpleFileStoreProcessor {

  private val dataChunkSize : Int = BackupSystemConfig()("processing.ChunkSize", 524288)    // 512 kB
  private val maxChunkSize : Int = BackupSystemConfig()("processing.MaxChunkSize", 3145728) //   3 MB
  assert (maxChunkSize >= dataChunkSize)

  // EVENTUALLY make a repository settings
  private val headerChunkSize : Int = 1024
  private def newHeaderDigester() = Digester.crcadler()
  private def newHashDigester() = Digester.hash("MD5")
  assert (dataChunkSize >= headerChunkSize)
  
  // EVENTUALLY implement storage database (two tables: id/header/hash/sourceSize and id/index/position/storageSize
  case class StoreEntry(id : Long)
  case class StoredPiece(position: Long, size: Long)
  trait StoreMethod
  object Uncompressed extends StoreMethod
  object Deflated extends StoreMethod
  case class StoredData(id: Long, size: Long, method: StoreMethod, pieces : List[StoredPiece])
  def dbContains(size: Long, headerChecksum: Checksum) : Boolean = false
  def dbLookup(size: Long, headerChecksum: Checksum, hash: Bytes) : Option[StoreEntry] = None
  def dbCreateEntry(size: Long, headerChecksum: Checksum, hash: Bytes) : StoreEntry = StoreEntry(0)
  def dbDeleteEntry(entry: StoreEntry) = Unit
  def dbCompleteEntry(entry: StoreEntry, size: Long, headerChecksum: Checksum, hash: Bytes, storedData: StoredData) : StoreEntry = StoreEntry(0)
  def dbTransaction[T](task: => T) = task

  // EVENTUALLY implement storage
  /** @throws Exception on error state in storage. */
  def store(data: Iterator[Bytes]) : StoredData = StoredData(0, 0, Uncompressed, Nil)
  def release(data: StoredData) : Unit = Unit
  
  
  // Note: May not be called at all for files that have the same
  // signature as during a previous backup.
  // Note: When updating a previous backup, it is a good idea to
  // call directly storeProbablyKnown or storeProbablyNew based
  // on e.g. whether the modification time stamp matches as well.
  def linkOrStore(file: File) : StoreEntry =
    Closeable.usingIt(new RandomAccessFileInput(file)) { input =>
      // read completely to memory if not too large
      val readSize =
        if (input.length <= maxChunkSize) input.length.toInt + 1
        else math.max(headerChunkSize, dataChunkSize)
      val firstChunk = input.readFully(readSize)
      val header = newHeaderDigester().writeAnd(firstChunk.keepAtMostFirst(headerChunkSize)).getDigest
      if (readSize > firstChunk.length)
        linkOrStoreFromMemory(firstChunk, header)
      else if (dbContains(input.length, header))
        storeProbablyKnown(input, iterateData(input, firstChunk), header)
      else
        storeProbablyNew(iterateData(input, firstChunk))
    }

  def linkOrStoreFromMemory(data: Bytes, header: Checksum) : StoreEntry = {
    val hash = newHashDigester().writeAnd(data).getDigest
    dbTransaction {
      dbLookup(data.length, header, hash) match {
        case Some(entry) => (true, entry)
        case None => (false, dbCreateEntry(data.length, header, hash))
      }
    } match {
      case (true, entry) => entry
      case (false, entry) =>
        val storedData =
          try { store(Iterator(data)) }
          catch { case e => dbDeleteEntry(entry) ; throw e }
        dbCompleteEntry(entry, data.length, header, hash, storedData)
    }
  }
  
  def iterateData(input: RandomAccessFileInput, firstChunk: Bytes) : Iterator[Bytes] =
    new Iterator[Bytes] {
      var current = firstChunk
      override def hasNext: Boolean = current.length > 0
      override def next: Bytes = {
        val result = current
        current = input.readFully(dataChunkSize)
        result
      }
    }
  
  def storeProbablyKnown(input: RandomAccessFileInput, iterator: Iterator[Bytes], header: Checksum) : StoreEntry = {
    val hashDigester = newHashDigester()
    val length = iterator.foldLeft(0L)((length, data) => { hashDigester.write(data) ; length + data.length })
    val hash = hashDigester.getDigest
    dbLookup(length, header, hash) match {
      case Some(entry) => entry
      case None =>
        input.seek(0)
        storeProbablyNew(iterateData(input, input.readFully(dataChunkSize)))
    }
    // EVENTUALLY add Resettable trait to RandomAccessFileInput, also to Digest(er)
  }

  def storeProbablyNew(iterator: Iterator[Bytes]) : StoreEntry = {
    val headerDigester = newHeaderDigester()
    val hashDigester = newHashDigester()
    val hashIterator = new Iterator[Bytes] {
      var firstRun = true
      override def hasNext: Boolean = iterator.hasNext
      override def next: Bytes = {
        val bytes = iterator.next
        hashDigester.write(bytes)
        if (firstRun) {
          headerDigester.write(bytes.keepAtMostFirst(headerChunkSize))
          firstRun = false
        }
        bytes
      }
    }
    val storedData = store(hashIterator)
    val header = headerDigester.getDigest
    val hash = hashDigester.getDigest
    dbTransaction {
      dbLookup(storedData.size, header, hash) match {
        case Some(entry) => release(storedData) ; entry
        case None => dbCreateEntry(storedData.size, header, hash)
      }
    }
    // EVENTUALLY add an option to avoid hash recalculation when called from storeProbablyKnown
  }
  
}