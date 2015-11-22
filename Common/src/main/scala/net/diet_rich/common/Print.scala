package net.diet_rich.common

import java.util.zip.{CRC32, Adler32}

class Print private(val value: Long) extends AnyVal with LongValue
object Print {
  def apply(value: Long): Print = new Print(value)

  // FIXME rename to "of"
  def printOf(data: Array[Byte], offset: Int, length: Int): Print= Print {
    init(new Adler32){_ update (data, offset, length)}.getValue << 32 |
      init(new CRC32  ){_ update (data, offset, length)}.getValue
  }
  def printOf(data: Array[Byte]): Print = printOf(data, 0, data.length)
  def printOf(bytes: Bytes): Print = printOf(bytes.data, bytes.offset, bytes.length)
  def printOf(string: String): Print = printOf(string getBytes "UTF-8")
}
