package net.diet_rich

import java.util.zip.{CRC32, Adler32}

package object common {
  def init[T](t: T)(f: T => Unit): T = { f(t); t }

  class Before[T](val t: T) extends AnyVal { def before(f: => Unit) = { f; t } }
  def valueOf[T](t: T) = new Before(t)

  def printOf(data: Array[Byte], offset: Int, length: Int): Long = {
    init(new Adler32){_ update (data, offset, length)}.getValue << 32 |
    init(new CRC32  ){_ update (data, offset, length)}.getValue
  }
  def printOf(data: Array[Byte]): Long = printOf(data, 0, data.length)
  def printOf(bytes: Bytes): Long = printOf(bytes.data, bytes.offset, bytes.length)
  def printOf(string: String): Long = printOf(string getBytes "UTF-8")

  def now = System.currentTimeMillis()

  type StringMap = String Map String
}
