// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.data

trait Digester[DigestType] extends net.diet_rich.util.io.OutputStream with Digest[DigestType]

object Digester {
  
  /** wrapped to distinguish it from e.g. a length or time stamp. */
  case class Checksum (value: Long)

  def checksum(checksum: java.util.zip.Checksum) : Digester[Checksum] = {
    new Digester[Checksum] {
      override def write(buffer: Array[Byte], offset: Int, length: Int) = 
        checksum.update(buffer, offset, length)
      override def getDigest = Checksum(checksum.getValue)
    }
  }
  
  def adler32 = checksum(new java.util.zip.Adler32)
  
  def crc32 = checksum(new java.util.zip.CRC32)

  def hash(algorithm: String) : Digester[Array[Byte]] = {
    val digest = java.security.MessageDigest.getInstance(algorithm)
    new Digester[Array[Byte]] {
      override def write(buffer: Array[Byte], offset: Int, length: Int) = 
        digest.update(buffer, offset, length)
      override def getDigest = digest.digest
    }
  }
  
}