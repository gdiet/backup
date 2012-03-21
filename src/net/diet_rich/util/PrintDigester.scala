package net.diet_rich.util

trait PrintDigester {
  def digest : Long
  def write(data: Bytes) : PrintDigester
}

object PrintDigester {
  def crcAdler : PrintDigester = new PrintDigester {
    val crc = new java.util.zip.CRC32
    val adler = new java.util.zip.Adler32
    def write(data: Bytes) : PrintDigester = {
      crc.update(data data, data intOffset, data intSize)
      adler.update(data data, data intOffset, data intSize)
      this
    }
    def digest : Long = adler.getValue << 32 | crc.getValue
  }
}