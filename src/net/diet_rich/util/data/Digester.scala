// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.data

trait Digester[DigestType] extends net.diet_rich.util.io.OutputStream with Digest[DigestType] {
  def reset : Unit
  final def writeAnd(bytes: Bytes) : Digester[DigestType] = { write(bytes); this }
}

object Digester {

  trait ChecksumDigester extends Digester[Long]
  trait BytesDigester extends Digester[Bytes]

  private def checksum(checksum: java.util.zip.Checksum, seed: Option[Bytes]) =
    new ChecksumDigester {
      if (seed.isDefined) reset
      override def write(bytes: Bytes) = { checksum.update(bytes.bytes, bytes.offset toInt, bytes.length toInt) }
      override def getDigest = checksum.getValue
      override def reset = { checksum.reset ; seed.foreach(write(_)) }
      override def close = Unit
    }
  
  def adler32(seed: Option[Long] = None) = checksum(new java.util.zip.Adler32, seed.map(Bytes.forLong(_)))
  
  def crc32(seed: Option[Long] = None) = checksum(new java.util.zip.Adler32, seed.map(Bytes.forLong(_)))

  def crcadler(seed: Option[Long] = None) =
    new ChecksumDigester {
      val crc = crc32(seed)
      val adler = adler32(seed)
      override def write(bytes: Bytes) = { crc.write(bytes) ; adler.write(bytes) }
      override def getDigest = adler.getDigest << 32 | crc.getDigest
      override def reset = { crc.reset ; adler.reset }
      override def close = Unit
    }
  
  def hash(algorithm: String, seed: Option[Long] = None) =
    new BytesDigester {
      val digest = java.security.MessageDigest.getInstance(algorithm)
      if (seed.isDefined) reset
      override def write(bytes: Bytes) = { digest.update(bytes.bytes, bytes.offset toInt, bytes.length toInt) }
      override def getDigest = Bytes(digest.digest)
      override def reset = { digest.reset ; seed.foreach(long => write(Bytes forLong long)) }
      override def close = Unit
    }

  def hash64(algorithm: String, seed: Option[Long] = None) =
    new ChecksumDigester {
      val digester = hash(algorithm, seed)
      override def write(bytes: Bytes) = { digester write bytes }
      override def getDigest = digester.getDigest longFrom
      override def reset = digester reset
      override def close = Unit
    }

}
