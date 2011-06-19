// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.storelogic

trait StoreLogic extends net.diet_rich.util.logging.Logged {
  import net.diet_rich.util.data.Digester
  import net.diet_rich.util.data.Digester.Checksum
  import net.diet_rich.util.io.{Closeable,ResettableInputStream}
  import net.diet_rich.util.io.Streams._
  import StoreLogic._

  // methods to implement when trait is used
  
  val headerSize : Int
  def newHeaderDigester() : Digester[Checksum]
  def newHashDigester() : Digester[DataHash]
  def dbContains(size: Long, headerChecksum: Checksum) : Boolean
  def dbLookup(size: Long, headerChecksum: Checksum, hash: DataHash) : Option[DataLocation]
  def dbMarkDeleted(data: DataLocation)
  def newStoreStream() : Digester[DataLocation] with Closeable

  // store logic
  
  /**
   * Note: May not be called at all for files that have the same
   * size and time stamp as during a previous backup.
   */
  def storeOrLink(input: ResettableBackupInput) : DataLocation = {
    debug("store or link", input.sourceForLog)
    val headerChecksum = getHeaderChecksum(input)
    if (!dbContains(input.length, headerChecksum)) {
      storeNow(input, headerChecksum, None)
    } else {
      val (length, headerChecksum2, hash) = getHeaderChecksumAndHash(input)
      if (length != input.length) warning("source length change", input.sourceForLog)
      if (headerChecksum != headerChecksum2) warning("source header change", input.sourceForLog)
      dbLookup(input.length, headerChecksum, hash)
        .getOrElse(storeNow(input, headerChecksum2, Option(hash)))
    }
  }
  
  private def storeNow(input: ResettableBackupInput, initialHeaderChecksum: Checksum, initialHash: Option[DataHash]) : DataLocation = {
    debug("store input", input.sourceForLog)
    val (length, headerChecksum, hash, location) = executeStore(input)
    val warnkey = if (length != input.length) "detected source length change"
      else if (headerChecksum != initialHeaderChecksum) "detected source header change"
      else if (!initialHash.forall(_ sameAs hash)) "detected source hash change"
      else ""
    if (!warnkey.isEmpty) {
      warning(warnkey, input.sourceForLog)
      markDeletedIfNecessary(input, length, headerChecksum, hash, location)
    } else location
  }

  private def markDeletedIfNecessary(input: ResettableBackupInput, length: Long, headerChecksum: Checksum, hash: DataHash, location: DataLocation) : DataLocation = {
    dbLookup(length, headerChecksum, hash) match {
      case None => 
        location
      case Some(previousLocation) =>
        dbMarkDeleted(location)
        info("marked duplicate data obsolete", input.sourceForLog)
        previousLocation
    } 
  }
  
  private def executeStore(input: ResettableBackupInput) : (Long, Checksum, DataHash, DataLocation) = {
    input.reset
    val hashStream = digestStream(input, newHashDigester)
    val checksumStream = digestStream(hashStream, newHeaderDigester)
    val storeStream = newStoreStream
    val length = copyData(checksumStream, storeStream, headerSize) + copyData(hashStream, storeStream)
    storeStream.close
    (length, checksumStream.getDigest, hashStream.getDigest, storeStream.getDigest)
  }
  
  private def getHeaderChecksum(input: ResettableInputStream) : Checksum = {
    input.reset
    val checksumStream = digestStream(input, newHeaderDigester)
    readSkip(checksumStream, headerSize)
    checksumStream.getDigest
  }

  private def getHeaderChecksumAndHash(input: ResettableInputStream) : (Long, Checksum, DataHash) = {
    input.reset
    val hashStream = digestStream(input, newHashDigester)
    val checksumStream = digestStream(hashStream, newHeaderDigester)
    val length = readSkip(checksumStream, headerSize) + readSkip(hashStream)
    (length, checksumStream.getDigest, hashStream.getDigest)
  }
}


object StoreLogic {
  import net.diet_rich.util.io._

  trait DataHash {
    def sameAs(other: DataHash) : Boolean
    def notSameAs(other: DataHash) : Boolean = !sameAs(other)
  }
  
  trait ResettableBackupInput extends ResettableInputStream {
    /** used for logging purposes only */
    val sourceForLog : AnyRef
    def length : Long
  }

  trait DataLocation // TODO implement
  
}