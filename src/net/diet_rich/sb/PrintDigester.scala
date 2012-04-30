package net.diet_rich.sb

import java.security.MessageDigest
import java.io.InputStream
import java.security.DigestInputStream
import java.io.FilterInputStream
import java.io.ByteArrayInputStream

trait PrintDigester extends InputStream {
  def print: Long
}

object PrintDigester {
  val zeroBytePrint: Long = PrintDigester(0, new ByteArrayInputStream(new Array(0))).print
  
  def apply(length: Int, stream: InputStream) : PrintDigester = new FilterInputStream(stream) with PrintDigester {
    val header = new Array[Byte](length)
    val headerLength = {
      def readRecurse(offset: Int): Int = {
        in.read(header, offset, length - offset) match {
          case -1 => offset
          case  n => readRecurse(offset)
        }
      }
      readRecurse(0)
    }
    override val print: Long = {
      val crc = new java.util.zip.CRC32
      val adler = new java.util.zip.Adler32
      crc.update(header, 0, headerLength)
      adler.update(header, 0, headerLength)
      adler.getValue << 32 | crc.getValue
    }
    val input = new java.io.SequenceInputStream(new ByteArrayInputStream(header, 0, headerLength), in)
    override def read: Int = input.read
    override def read(bytes: Array[Byte], offset: Int, length: Int): Int = input.read(bytes, offset, length)
  }
}
