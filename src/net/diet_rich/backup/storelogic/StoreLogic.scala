// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.storelogic

trait StoreLogic extends net.diet_rich.util.logging.Logged {
  import net.diet_rich.util.data.Digester
  import net.diet_rich.util.io.{Streams,ResettableInputStream}
  import StoreLogic._

  // methods to implement when trait is used
  
  val headerSize : Int
  def newHeaderDigester() : Digester[Long]
  def newHashDigester() : Digester[Array[Byte]]
  def dbContains(size: Long, lastModified: Long, headerChecksum: Long) : Boolean

  
  // store logic
  
  /**
   * Note: May not be called at all for files that have the same
   * size and time stamp as during a previous backup.
   */
  def storeOrLink(input: ResettableBackupInput) : DataLocation = {
    val headerChecksum = getHeaderChecksum(input)
    if (dbContains(input.length, input.lastModified, headerChecksum)) {
      val (length, headerChecksum2, hash) = getHeaderChecksumAndHash(input)
      1
      // found => calculate header checksum and hash
      // look up size, header checksum and hash in database
      // found => return link data
      // not found => store immediately, while calculating header checksum and hash
      // if size, header checksum or hash hash changed
      // => warning, check again against database, eventually mark as deleted
      // return link data
    } else {
      storeNow(input, headerChecksum, None)
    }
    throw new AssertionError
  }
  
  private def storeNow(input: ResettableBackupInput, initialHeaderChecksum: Long, initialHash: Option[Array[Byte]]) : DataLocation = {
    // store, calculate header checksum and hash and compare with initial values if available
    throw new AssertionError
  }
  
  private def getHeaderChecksum(input: ResettableInputStream) : Long = {
    input.reset
    val checksumStream = Streams.digestStream(input, newHeaderDigester)
    Streams.readSkip(checksumStream, headerSize)
    checksumStream.getDigest
  }

  private def getHeaderChecksumAndHash(input: ResettableInputStream) : (Long, Long, Array[Byte]) = {
    input.reset
    val hashStream = Streams.digestStream(input, newHashDigester)
    val checksumStream = Streams.digestStream(hashStream, newHeaderDigester)
    val headerBytesRead = Streams.readSkip(checksumStream, headerSize)
    val bodyBytesRead = Streams.readSkip(hashStream)
    (headerBytesRead + bodyBytesRead, checksumStream.getDigest, hashStream.getDigest)
  }
  
}

object StoreLogic {
  import net.diet_rich.util.io._

  trait ResettableBackupInput extends ResettableInputStream {
    def lastModified : Long
    def length : Long
  }

  trait DataLocation
  
}