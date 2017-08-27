package net.diet_rich.common.vals

import java.util.zip.{Adler32, CRC32}

import net.diet_rich.common.{Bytes, init}

/** A fast-to-calculate 64-bit hash value. */
class Print private(val value: Long) extends AnyVal with LongValue
object Print {
  def apply(value: Long): Print = new Print(value)

  def of(data: Array[Byte], offset: Int, length: Int): Print= Print {
    init(new Adler32){_ update (data, offset, length)}.getValue << 32 |
      init(new CRC32  ){_ update (data, offset, length)}.getValue
  }
  def of(data: Array[Byte]): Print = of(data, 0, data.length)
  def of(bytes: Bytes): Print = of(bytes.data, bytes.offset, bytes.length)
  def of(string: String): Print = of(string getBytes "UTF-8")

  val empty: Print = of(Array.empty[Byte])
}
