package net.diet_rich.fdfs

import net.diet_rich.util.io._
import net.diet_rich.util.Bytes

object PrintCalculator {
  val PRINTAREA = 2048
  
  val zeroBytePrint: Long = print (new java.io.ByteArrayInputStream(new Array(0)), new Array(PRINTAREA)) ._2

  def print(input: Reader, bytes: Array[Byte]) : (Int, Long) = {
    val read = readFully(input, bytes, 0, PRINTAREA)
    val crc = new java.util.zip.CRC32
    val adler = new java.util.zip.Adler32
    crc.update(bytes, 0, read)
    adler.update(bytes, 0, read)
    (read, adler.getValue << 32 | crc.getValue)
  }
  
  def print(input: Reader) : (Bytes, Long) = {
    val bytes = new Array[Byte](PRINTAREA)
    val read = readFully(input, bytes, 0, PRINTAREA)
    val crc = new java.util.zip.CRC32
    val adler = new java.util.zip.Adler32
    crc.update(bytes, 0, read)
    adler.update(bytes, 0, read)
    (Bytes(bytes, 0, read), adler.getValue << 32 | crc.getValue)
  }
}

class HashCalculator(input: Reader, algorithm: String) {
  // FIXME check in advance that the MessageDigest objects are cloneable
  protected val digester = java.security.MessageDigest.getInstance(algorithm)
  def hash = digester.digest
  def digestClone: java.security.MessageDigest = digester.clone.asInstanceOf[java.security.MessageDigest]
  def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
    val result = input.read(bytes, offset, length)
    if (result > 0) digester.update(bytes, offset, result)
    result
  }
}

object HashCalculator {
  def zeroByteHash(algorithm: String): Array[Byte] = java.security.MessageDigest.getInstance(algorithm).digest
}
