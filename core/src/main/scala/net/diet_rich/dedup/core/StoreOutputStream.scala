package net.diet_rich.dedup.core

import java.io.{IOException, OutputStream}
import java.util.Arrays.copyOfRange
import java.util.concurrent.ArrayBlockingQueue

import net.diet_rich.dedup.core.data.Bytes
import net.diet_rich.dedup.util.{resultOf, valueOf}

// TODO eventually check reactive stream like logic
class StoreOutputStream(storeLogic: StoreLogicBackend, processDataid: Long => Unit) extends OutputStream {
  def write(bytes: Bytes): Unit = write(bytes.data, bytes.offset, bytes.length)
  override def write(byte: Int): Unit = write(Array(byte.toByte), 0, 1)
  override def write(data: Array[Byte]): Unit = write(data, 0, data.length)

  protected val transfer = new ArrayBlockingQueue[Bytes](8)
  protected var closed = false
  override def write(data: Array[Byte], offset: Int, length: Int): Unit = {
    if (closed) throw new IOException("Output stream is already closed")
    if (length > 0) transfer put Bytes(copyOfRange(data, offset, offset+length), 0, length)
  }
  override def close(): Unit = if (!closed) {
    closed = true
    transfer put Bytes.empty
    processDataid(resultOf(dataid))
  }

  protected val dataid = storeLogic futureDataidFor theSource

  object theSource extends Source {
    protected var pool: Option[Array[Byte]] = Some(Array())
    @annotation.tailrec
    override final def read(count: Int): Bytes = pool match {
      case None => Bytes.empty
      case Some(bytes) =>
        if (bytes.length >= count) {
          valueOf(Bytes(bytes, 0, count)) before {pool = Some(bytes drop count)}
        } else {
          transfer.take() match {
            case Bytes.empty =>
              pool = None
              Bytes(bytes, 0, bytes.length)
            case newData =>
              pool = Some(bytes ++ newData.asByteArray)
              read(count)
          }
        }
      }
  }
}
