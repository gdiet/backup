// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.data

import net.diet_rich.util.Bytes

trait Digester[DigestType, +Repr] extends net.diet_rich.util.io.OutputStream[Repr] with Digest[DigestType] {
  def reset : Unit
}

/** Wrapped to distinguish it from e.g. a length or time stamp. */
case class Checksum (value: Long)

trait ChecksumDigester extends Digester[Checksum, ChecksumDigester]
trait BytesDigester extends Digester[Bytes, BytesDigester]

object Digester {
  

  private def checksum(checksum: java.util.zip.Checksum, seed: Option[Bytes]) =
    new ChecksumDigester {
      if (seed.isDefined) reset
      override def write(bytes: Bytes) = { checksum.update(bytes.bytes, bytes.offset, bytes.length) ; this }
      override def getDigest = Checksum(checksum.getValue)
      override def reset = { checksum.reset ; seed.foreach(write(_)) }
    }
  
  def adler32(seed: Option[Long] = None) = checksum(new java.util.zip.Adler32, seed.map(Bytes.forLong(_)))
  
  def crc32(seed: Option[Long] = None) = checksum(new java.util.zip.Adler32, seed.map(Bytes.forLong(_)))

  def crcadler(seed: Option[Long] = None) =
    new ChecksumDigester {
      val crc = crc32(seed)
      val adler = adler32(seed)
      override def write(bytes: Bytes) = { crc.write(bytes) ; adler.write(bytes) }
      override def getDigest = Checksum(adler.getDigest.value << 32 | crc.getDigest.value)
      override def reset = { crc.reset ; adler.reset }
    }
  
  def hash(algorithm: String, seed: Option[Long] = None) =
    new BytesDigester {
      val digest = java.security.MessageDigest.getInstance(algorithm)
      if (seed.isDefined) reset
      override def write(bytes: Bytes) = { digest.update(bytes.bytes, bytes.offset, bytes.length) ; this }
      override def getDigest = Bytes(digest.digest)
      override def reset = { digest.reset ; seed.foreach(long => write(Bytes.forLong(long))) }
    }

  def hash64(algorithm: String, seed: Option[Long] = None) =
    new ChecksumDigester {
      val digester = hash(algorithm, seed)
      override def write(bytes: Bytes) = { digester.write(bytes) ; this }
      override def getDigest = Checksum(digester.getDigest.keepFirst(8).toLong)
      override def reset = digester.reset
    }

}
