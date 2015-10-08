package net.diet_rich.dedupfs

import java.util.zip.{Inflater, Deflater}

import net.diet_rich.common._

object StoreMethod {
  val STORE = 0
  val DEFLATE = 1
  val named = Map (
    "store" -> STORE,
    "deflate" -> DEFLATE
  )
  val names = named map (_.swap)
  assert (named.size == names.size)

  type Coder = Iterator[Bytes] => Iterator[Bytes]

  val storeCoder: Int Map Coder = Map(
    STORE -> identity,
    DEFLATE -> (data => process(new DeflatePacker, data))
  )

  val restoreCoder: Int Map Coder = Map(
    STORE -> identity,
    DEFLATE -> (data => process(new InflatePacker, data))
  )

  private trait Packer {
    def setInput(data: Bytes): Unit
    def needsInput: Boolean
    def finish(): Unit
    def finished: Boolean
    def getOutput(data: Array[Byte], offset: Int, length: Int): Int
  }

  private class DeflatePacker extends Packer {
    private val deflater = new Deflater(Deflater.BEST_COMPRESSION, true)
    override def setInput(bytes: Bytes) = deflater.setInput(bytes.data, bytes.offset, bytes.length)
    override def needsInput = deflater.needsInput
    override def finish() = deflater finish()
    override def finished = deflater.finished
    override def getOutput(data: Array[Byte], offset: Int, length: Int) = deflater deflate (data, offset, length)
  }

  private class InflatePacker extends Packer {
    private val inflater = new Inflater(true)
    override def setInput(bytes: Bytes) = inflater.setInput(bytes.data, bytes.offset, bytes.length)
    override def needsInput = inflater.needsInput
    override def finish() = inflater setInput Array[Byte](0) // see javadoc of public Inflater(boolean nowrap)
    override def finished = inflater.finished
    override def getOutput(data: Array[Byte], offset: Int, length: Int) = inflater inflate (data, offset, length)
  }

  private val compressorChunkSize = 0x10000

  private def process(packer: Packer, data: Iterator[Bytes]) = new Iterator[Bytes] {
    var nextBytes: Option[Bytes] = None

    def refill() = if (packer.needsInput) {
      if (data.hasNext) packer setInput data.next else packer finish()
    }

    @annotation.tailrec
    def read(chunk: Option[Bytes]): Option[Bytes] =
      if (packer.finished) chunk else chunk match {
        case None => read(Some(Bytes zero compressorChunkSize copy(length = 0)))
        case result @ Some(Bytes(_, 0, `compressorChunkSize`)) => result
        case Some(Bytes(chunkData, 0, length)) =>
          refill()
          val sizeRead = packer getOutput (chunkData, length, compressorChunkSize - length)
          read(Some(Bytes(chunkData, 0, length + sizeRead)))
        case _ => sys.error(s"chunk did not match: $chunk")
      }

    override def hasNext: Boolean = nextBytes.isDefined || {
      nextBytes = read(None)
      nextBytes.isDefined
    }

    override def next(): Bytes = valueOf(nextBytes.get) before { nextBytes = None }
  }
}
