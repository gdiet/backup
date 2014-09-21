// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

import java.util.zip.{Deflater, Inflater}

import net.diet_rich.dedup.util.valueOf

trait StoreMethod extends IntValue {
  def pack(data: Iterator[Bytes]): Iterator[Bytes]
  def unpack(data: Iterator[Bytes]): Iterator[Bytes]
}

object StoreMethod {
  def apply(value: Int) = value match {
    case 0 => STORE
    case 1 => DEFLATE
  }

  case object STORE extends StoreMethod {
    override val value = 0
    override def pack(data: Iterator[Bytes]) = data
    override def unpack(data: Iterator[Bytes]) = data
  }

  case object DEFLATE extends StoreMethod {
    override val value = 1
    override def pack(data: Iterator[Bytes]) = process(new DeflatePacker, data)
    override def unpack(data: Iterator[Bytes]) = process(new InflatePacker ,data)

    private val compressorChunkSize = 0x10000

    trait Packer {
      def setInput(data: Bytes): Unit
      def needsInput: Boolean
      def finish: Unit
      def finished: Boolean
      def getOutput(data: Array[Byte], offset: Int, length: Int): Int
    }

    class DeflatePacker extends Packer {
      private val deflater = new Deflater(Deflater.BEST_COMPRESSION, true)
      override def setInput(data: Bytes) = deflater setInput data
      override def needsInput = deflater needsInput
      override def finish = deflater finish
      override def finished = deflater finished
      override def getOutput(data: Array[Byte], offset: Int, length: Int) = deflater deflate (data, offset, length)
    }

    class InflatePacker extends Packer {
      private val inflater = new Inflater(true)
      override def setInput(data: Bytes) = inflater setInput data
      override def needsInput = inflater needsInput
      override def finish = inflater setInput Array[Byte](0) // see javadoc of public Inflater(boolean nowrap)
      override def finished = inflater finished
      override def getOutput(data: Array[Byte], offset: Int, length: Int) = inflater inflate (data, offset, length)
    }

    def process(packer: Packer, data: Iterator[Bytes]) = new Iterator[Bytes] {
      var nextBytes: Option[Bytes] = None

      def refill = if (packer needsInput) if (data.hasNext) packer setInput data.next else packer finish

      @annotation.tailrec
      def read(chunk: Option[Bytes]): Option[Bytes] =
        if (packer finished) chunk else chunk match {
          case None => read(Some(Bytes zero compressorChunkSize withSize 0))
          case result @ Some(Bytes(_, 0, `compressorChunkSize`)) => result
          case Some(Bytes(data, 0, length)) =>
            refill
            val sizeRead = packer getOutput (data, length, compressorChunkSize - length)
            read(Some(Bytes(data, 0, length + sizeRead)))
          case _ => sys.error(s"chunk did not match: $chunk")
        }

      def hasNext: Boolean = nextBytes.isDefined || {
        nextBytes = read(None)
        nextBytes.isDefined
      }

      def next(): Bytes = valueOf (nextBytes.get) before { nextBytes = None }
    }
  }
}
