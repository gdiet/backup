// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import akka.dispatch.{Future,Futures}
import java.io.File
import net.diet_rich.util.Bytes
import net.diet_rich.util.data.{Checksum,Digester}
import net.diet_rich.util.io.{OutputStream,RandomAccessFileInput}

class SimpleFileStoreProcessor {

  private val dataChunkSize : Int = BackupSystemConfig()("processing.ChunkSize", 524288)    // 512 kB
  private val maxChunkSize : Int = BackupSystemConfig()("processing.MaxChunkSize", 3145728) //   3 MB
  assert (maxChunkSize >= dataChunkSize)

  // FIXME make a repository settings
  private val headerChunkSize : Int = 1024
  private def newHeaderDigester() = Digester.crcadler()
  private def newHashDigester() = Digester.hash("MD5")
  assert (maxChunkSize >= headerChunkSize)
  
  // FIXME implement database
  case class StoreEntry(id : Long)
  def dbContains(size: Long, headerChecksum: Checksum) : Boolean = false
  def dbLookup(size: Long, headerChecksum: Checksum, hash: Bytes) : Option[StoreEntry] = None

  // FIXME implement others
//  def byteStoreWriter()
  
  type PossibleProblem = Future[Option[Throwable]]

  // Note: May not be called at all for files that have the same
  // signature as during a previous backup.
  // Note: When updating a previous backup, it is a good idea to
  // call directly storeProbablyKnown or storeProbablyNew based
  // on e.g. whether the modification time stamp matches as well.
  def linkOrStore(file: File) : StoreEntry = {
    // FIXME close input (e.g. with "with closable" method)
    val input = new RandomAccessFileInput(file)
    // read completely to memory if not too large
    val readSize =
      if (input.length <= maxChunkSize)
        input.length.toInt + 1
      else
        math.max(headerChunkSize, dataChunkSize)
    val firstChunk = Bytes(readSize)
    val allRead = input.readFully(firstChunk) < firstChunk.length
    val headerDigester = newHeaderDigester()
    headerDigester.write(firstChunk.keepAtMostFirst(headerChunkSize))
    val headerChecksum = headerDigester.getDigest
    if (allRead)
      linkOrStoreFromMemory(firstChunk, headerChecksum)
    else if (dbContains(input.length, headerChecksum))
      storeProbablyKnown(input, firstChunk, headerChecksum)
    else
      storeProbablyNew(input, firstChunk, headerChecksum)
  }

  def linkOrStoreFromMemory(data: Bytes, headerChecksum: Checksum) : StoreEntry = {
    val hashDigester = newHashDigester()
    hashDigester.write(data)
    val hash = hashDigester.getDigest
    dbLookup(data.length, headerChecksum: Checksum, hash: Bytes) match {
      case Some(entry) => entry
      case None => StoreEntry(0) // FIXME
    }
  }
  
  def storeProbablyKnown(input: RandomAccessFileInput, header: Bytes, headerChecksum: Checksum) : StoreEntry = {
    // FIXME use Stream?
    
    // FIXME read twice if necessary
    // FIXME add Resettable trait, also to Digest(er)
    // FIXME add copy input -> output method
    
//    val hashDigester = newHashDigester()
//    hashDigester.write(header)
//    // read completely to memory if not too large
//    val secondChunkSize = if (input.length < maxDataSize) 
    
    StoreEntry(0) // FIXME
  }

  def storeProbablyNew(input: RandomAccessFileInput, header: Bytes, headerChecksum: Checksum) : StoreEntry = {
    // FIXME use Stream?
    
//    val tee = OutputStream.tee
    // EVENTUALLY add an option to avoid hash recalculation when called from storeProbablyKnown
    StoreEntry(0) // FIXME
  }
  
}