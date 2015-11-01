package net.diet_rich.dedupfs

import java.io.{IOException, OutputStream}
import java.util.Arrays.copyOfRange

import net.diet_rich.common._, io.Source

class StoreOutputStream(storeLogic: StoreLogic, maxBytesToCache: Long, processDataid: Long => Unit) extends OutputStream {
  private var closed = false
  private var cached = 0L
  private var cache = Vector[Bytes]()
  override def write(byte: Int): Unit = write(Array(byte.toByte), 0, 1)
  override def write(data: Array[Byte]): Unit = write(data, 0, data.length)
  override def write(data: Array[Byte], offset: Int, length: Int): Unit = {
    if (closed) throw new IOException("Output stream is already closed")
    if (cached + length > maxBytesToCache) throw new IOException("Output stream exceeded cache size limit") // TODO implement plan B
    if (length > 0) cache = cache :+ Bytes(copyOfRange(data, offset, offset+length), 0, length)
  }
  override def close(): Unit = if (!closed) {
    closed = true
    processDataid(storeLogic dataidFor theSource)
  }
  private object theSource extends Source {
    override def read(count: Int): Bytes = {
      cache.headOption match {
        case None => Bytes.empty
        case Some(bytes) =>
          if (bytes.length <= count) bytes
          else valueOf(bytes.withLength(count)) before {cache = bytes.addOffset(count) +: cache.tail}
      }
    }
  }
}
