// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.storelogic

import net.diet_rich.util.data.Digester
import net.diet_rich.util.data.Digester.Checksum
import net.diet_rich.util.io.{Closeable,ResettableInputStream}
import net.diet_rich.util.io.Streams._
import StoreLogic._

trait StoreLogic extends net.diet_rich.util.logging.Logged {

  // methods to implement when trait is used
  
  // TODO it seems it would be ok to have the following data table:
  // pk ; size ; headerChecksum ; hash ; [data location columns] ; markedDeleted
  // unique: size ; headerChecksum ; hash
  // requests would be for:
  // (size & headerChecksum) => Boolean
  // (size & headerChecksum & hash) => Option[pk]
  // (markedDeleted) => ???
  // (data file) => ???
  // the data location columns will be something like
  // partNumber ; fileID ; locationInFile ; length
  
  val headerSize : Int
  def newHeaderDigester() : Digester[Checksum]
  def newHashDigester() : Digester[DataHash]
  def dbContains(size: Long, headerChecksum: Checksum) : Boolean
  def dbLookup(size: Long, headerChecksum: Checksum, hash: DataHash) : Option[Long]
  /** @return left if already in database, right for new entry. */
  def dbStoreLocation(size: Long, headerChecksum: Checksum, hash: DataHash, data: DataLocation) : Either[Long, Long]
  def dbMarkDeleted(data: DataLocation)
  def newStoreStream() : Digester[DataLocation] with Closeable

  // store logic
  
  /**
   * Note: May not be called at all for files that have the same
   * size and time stamp as during a previous backup.
   * 
   * @return the database entry id for the data
   */
  def storeOrLink(input: ResettableBackupInput) : Long = {
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
  
  private def storeNow(input: ResettableBackupInput, initialHeaderChecksum: Checksum, initialHash: Option[DataHash]) : Long = {
    debug("store input", input.sourceForLog)
    val (length, headerChecksum, hash, location) = executeStore(input)
    val warnkey = if (length != input.length) "detected source length change"
      else if (headerChecksum != initialHeaderChecksum) "detected source header change"
      else if (!initialHash.forall(_ sameElements hash)) "detected source hash change"
      else ""
    if (!warnkey.isEmpty) warning(warnkey, input.sourceForLog)
    dbStoreLocation(length, headerChecksum, hash, location).fold(
        oldPK => { dbMarkDeleted(location); oldPK },
        newPK => newPK)
  }

//  private def markDeletedIfNecessary(input: ResettableBackupInput, length: Long, headerChecksum: Checksum, hash: DataHash, location: DataLocation) : DataLocation = {
//    dbLookup(length, headerChecksum, hash) match {
//      case None => 
//        location
//      case Some(previousLocation) =>
//        dbMarkDeleted(location)
//        info("marked duplicate data obsolete", input.sourceForLog)
//        previousLocation
//    } 
//  }
  
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
  
  type DataHash = Array[Byte]
  
  trait ResettableBackupInput extends ResettableInputStream {
    /** used for logging purposes only */
    val sourceForLog : AnyRef
    def length : Long
  }

  trait DataLocation // TODO implement
  
  trait FileSignature {
    def size: Long
    def headerChecksum: Checksum
    def hash: Array[Byte]
  }
  
}