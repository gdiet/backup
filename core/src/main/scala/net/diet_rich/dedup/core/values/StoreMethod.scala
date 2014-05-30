// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

import java.util.zip.Deflater

import net.diet_rich.dedup.util._

trait StoreMethod extends IntValue {
  def pack(data: Iterator[Bytes]): Iterator[Bytes]
  def unpack(data: Iterator[Bytes]): Iterator[Bytes]
}

object StoreMethod {
  private abstract class Valued(val value: Int) extends StoreMethod
  private val compressorChunkSize = 0x10000

  def apply(value: Int) = value match {
    case 0 => STORE
    case 1 => DEFLATE
  }

  val STORE: StoreMethod = new Valued(0) {
    def pack(data: Iterator[Bytes]) = data
    def unpack(data: Iterator[Bytes]) = data
  }

  val DEFLATE: StoreMethod = new Valued(1) {
    def pack(data: Iterator[Bytes]) = new Iterator[Bytes] {
      var nextBytes: Option[Bytes] = None

      val deflater = new Deflater(Deflater.BEST_COMPRESSION, true)

      def refill = if (deflater needsInput) if (data.hasNext) deflater setInput data.next else deflater finish

      @annotation.tailrec
      def read(chunk: Option[Bytes]): Option[Bytes] =
        if (deflater finished) chunk else chunk match {
          case None => read(Some(Bytes(compressorChunkSize)))
          case result @ Some(Bytes(_, _, `compressorChunkSize`)) => result
          case Some(Bytes(data, 0, length)) =>
            refill
            val sizeRead = deflater.deflate(data, length, compressorChunkSize - length)
            read(Some(Bytes(data, 0, length + sizeRead)))
          case _ => sys.error(s"chunk did not match: $chunk")
        }

      def hasNext: Boolean = {
        nextBytes = read(None)
        nextBytes.isDefined
      }

      def next(): Bytes = nextBytes.get
    }

    def unpack(data: Iterator[Bytes]) = ???
  }
}
