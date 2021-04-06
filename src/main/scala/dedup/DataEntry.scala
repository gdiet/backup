package dedup

import jnr.ffi.Pointer

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.StandardOpenOption.{CREATE_NEW, READ, SPARSE, WRITE}
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.immutable.TreeMap

object DataEntry extends ClassLogging {
  // see https://stackoverflow.com/questions/13713557/scala-accessing-protected-field-of-companion-objects-trait
  @inline override protected def trace_(msg: => String): Unit = super.trace_(msg)
  @inline override protected def debug_(msg: => String): Unit = super.debug_(msg)
  @inline override protected def info_ (msg: => String): Unit = super.info_ (msg)
  @inline override protected def warn_ (msg: => String): Unit = super.warn_ (msg)
  @inline override protected def error_(msg:    String): Unit = super.error_(msg)
  @inline override protected def error_(msg: String, e: Throwable): Unit = super.error_(msg, e)

  protected val currentId = new AtomicLong()
  protected val closedEntries = new AtomicLong()
  def openEntries: Long = currentId.get - closedEntries.get

  val cacheLimit: Long = math.max(16000000, (Runtime.getRuntime.maxMemory - 64000000) * 7 / 10)
  val cacheUsed = new AtomicLong(0)
}

/** mutable! baseDataId can be -1. */
class DataEntry(val baseDataId: Long, initialSize: Long, tempDir: Path) extends AutoCloseable { import DataEntry._
  private val id = currentId.incrementAndGet()
  trace_(s"Create $id with base data ID $baseDataId.")

  private var maybeChannel: Option[SeekableByteChannel] = None
  private val path = tempDir.resolve(s"$id")
  private def channel = maybeChannel.getOrElse {
    debug_(s"Create cache file $path")
    Files.newByteChannel(path, WRITE, CREATE_NEW, SPARSE, READ).tap(c => maybeChannel = Some(c))
  }

  private var _written: Boolean = false
  def written: Boolean = synchronized(_written)

  private var _size: Long = initialSize
  def size: Long = synchronized(_size)

  private var stored = TreeMap[Long, Long]()

  /** readUnderlying is (dataId, offset, size, sink) */
  def read(offset: Long, size: Int, sink: Pointer, readUnderlying: (Long, Long, Int, Pointer) => Unit): Boolean = synchronized {
    if (size > _size - offset) {
      warn_(s"Requested size $size larger than available bytes between $offset and ${_size}.")
      false
    } else {
      var position = offset
      var parts = stored.filter { case (_, to) => to > position }
      while(position < size) {
        val (from, to) = parts.head
        parts = parts.tail

      }
      ???
    }
//    for (p <- position until position + sizeToRead by memChunk; chunkSize = math.min(memChunk, size - position).toInt) {
//      yield 1
//    }
  }

  def truncate(size: Long): Unit = synchronized { if (size != _size) {
    stored = stored.collect { case (from, to) if from < size => from -> math.min(to, size) }
    _written = true
    _size = size
  } }

  def write(offset: Long, size: Long, source: Pointer): Unit = synchronized {
    channel.position(offset)
    val chunk = new Array[Byte](memChunk)
    for (position <- 0L until size by memChunk; chunkSize = math.min(memChunk, size - position).toInt) {
      source.get(position, chunk, 0, chunkSize)
      channel.write(ByteBuffer.wrap(chunk, 0, chunkSize))
    }
    stored = stored + (offset -> (offset + size))
    val result -> last = stored.foldLeft(TreeMap[Long, Long]() -> (-1L, -1L)) {
      case ((result, last@(lastFrom, lastTo)), cur@(from,to)) =>
        if (lastTo >= from) result -> (lastFrom, to)
        else result + last -> cur
    }
    stored = result + last - (-1L)
    _written = true
    _size = math.max(_size, offset + size)
  }

  override def close(): Unit = synchronized {
    maybeChannel.foreach { channel =>
      channel.close()
      Files.delete(path)
      debug_(s"Closed & deleted cache file $path")
    }
    trace_(s"Close $id with base data ID $baseDataId.")
    closedEntries.incrementAndGet()
  }
}
