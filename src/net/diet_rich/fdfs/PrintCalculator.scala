package net.diet_rich.fdfs

import java.security.MessageDigest
import net.diet_rich.util.io._

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
}
