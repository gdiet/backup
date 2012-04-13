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
  val zeroBytePrint: Long = PrintDigester(new ByteArrayInputStream(new Array(0))).print
  
  def apply(stream: InputStream) : PrintDigester = new FilterInputStream(stream) with PrintDigester {
    val crc = new java.util.zip.CRC32
    val adler = new java.util.zip.Adler32
    override def print: Long = adler.getValue << 32 | crc.getValue
    override def read: Int = {
      val array = new Array[Byte](1)
      read(array) match {
        case -1 => -1
        case 1  => array(0)
        case _  => throw new IllegalStateException("unexpeced read result for single byte array")
      }
    }
    override def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
      in.read(bytes, offset, length) match {
        case -1 => -1
        case read =>
          crc.update(bytes, offset, read)
          adler.update(bytes, offset, read)
          read
      }
    }
  }
}
