// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

import scala.collection.mutable.MutableList

import net.diet_rich.dedup.util.{!!!, valueOf}

final case class Bytes(data: Array[Byte], offset: Int, length: Int) {
  def size: Size = Size(length)
  def withSize(size: Size): Bytes = withSize(size.value toInt)
  def withSize(length: Int): Bytes = {
    assume(length >= 0 && length <= this.length) // or length <= data.length - offset ?
    copy(length = length)
  }
  def withOffset(offset: Size): Bytes = withOffset(offset.value toInt)
  def withOffset(off: Int): Bytes = {
    assume(off >= 0 && off <= length)
    Bytes(data, offset + off, length - off)
  }
  // Note: Overridden so it is not used unwillingly, e.g. in matches
  override def equals(other: Any) = !!!
  private def theData = data drop offset take length
  // Note: Possibly slow, use with care
  def fullyEquals(other: Bytes) = length == other.length && java.util.Arrays.equals(theData, other.theData)
}

object Bytes extends ((Array[Byte], Int, Int) => Bytes) {
  val EMPTY = zero(0)

  // Note: MutableList allows in-place replacement when applying the store method to minimize memory impact
  def consumingIterator(data: MutableList[Bytes]): Iterator[Bytes] = new Iterator[Bytes] {
    var index = 0
    def hasNext: Boolean = index < data.size
    def next: Bytes = valueOf(data(index)) before {
      data.update(index, EMPTY)
      index += 1
    }
  }

  def zero(length: Int): Bytes = Bytes(new Array[Byte](length), 0, length)
  def zero(size: Size): Bytes = zero(size.value toInt)

  implicit class SizeOfBytesList(val data: Iterable[Bytes]) extends AnyVal {
    def sizeInBytes: Size = data.map(_.size).foldLeft(Size.Zero)(_+_)
  }

  implicit class WriteOutputStream(val u: java.io.OutputStream) extends AnyVal {
    def write(bytes: Bytes) = u.write(bytes.data, bytes.offset, bytes.length)
  }
  implicit class UpdateChecksum(val u: java.util.zip.Checksum) extends AnyVal {
    def update(bytes: Bytes) = u.update(bytes.data, bytes.offset, bytes.length)
  }
  implicit class UpdateMessageDigest(val u: java.security.MessageDigest) extends AnyVal {
    def update(bytes: Bytes) = u.update(bytes.data, bytes.offset, bytes.length)
  }
  implicit class UpdateDeflater(val u: java.util.zip.Deflater) extends AnyVal {
    def setInput(bytes: Bytes) = u.setInput(bytes.data, bytes.offset, bytes.length)
  }
  implicit class UpdateInflater(val u: java.util.zip.Inflater) extends AnyVal {
    def setInput(bytes: Bytes) = u.setInput(bytes.data, bytes.offset, bytes.length)
  }

  implicit class BytesReader(val bytes: Bytes) extends AnyVal {
    def fillFrom(input: (Array[Byte], Int, Int) => Int): Bytes = {
      import bytes._
      @annotation.tailrec
      def readRecurse(offset: Int, length: Int): Int =
        input(data, offset, length) match {
          case n if n < 1 => offset
          case n => if (n == length) offset + n else readRecurse(offset + n, length - n)
        }
      withSize(readRecurse(offset, length) - offset)
    }
  }
}
