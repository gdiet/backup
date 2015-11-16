package net.diet_rich.dedupfs

import java.io.{IOException, OutputStream}
import java.util.Arrays.copyOfRange
import java.util.concurrent.ArrayBlockingQueue

import net.diet_rich.common._, io.Source

class StoreOutputStream(storeLogic: StoreLogic, processDataid: Long => Unit) extends OutputStream {
  private var closed = false
  private val transfer = new ArrayBlockingQueue[Bytes](8)
  override def write(byte: Int): Unit = write(Array(byte.toByte), 0, 1)
  override def write(data: Array[Byte]): Unit = write(data, 0, data.length)
  override def write(data: Array[Byte], offset: Int, length: Int): Unit = {
    if (closed) throw new IOException("Output stream is already closed")
    if (length > 0) transfer put Bytes(copyOfRange(data, offset, offset+length), 0, length)
  }
  override def close(): Unit = if (!closed) {
    closed = true
    transfer put Bytes.empty
    processDataid(resultOf(dataid))
  }

  private val dataid = storeLogic futureDataidFor theSource

  private object theSource extends Source {
    protected var pool: Array[Byte] = Array()
    @annotation.tailrec
    override final def read(count: Int): Bytes = {
      if (pool.length >= count) {
        valueOf(Bytes(pool, 0, count)) before { pool = pool drop count }
      } else {
        transfer.take() match {
          case Bytes.empty =>
            transfer put Bytes.empty // Allows calling read multiple times when no more data available
            valueOf(Bytes(pool, 0, pool.length)) before { pool = Array() }
          case newData =>
            pool = pool ++ newData.asByteArray
            read(count)
        }
      }
    }
  }
}
