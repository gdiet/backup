package dedup

import dedup.cache.CombinedCache
import jnr.ffi.Pointer

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong

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

  val cacheLimit: Long = math.max(0, (Runtime.getRuntime.maxMemory - 64000000) * 7 / 10)
  val available = new AtomicLong(cacheLimit)
}

/** mutable! baseDataId can be -1. */
class DataEntry(val baseDataId: Long, initialSize: Long, tempDir: Path) extends AutoCloseable { import DataEntry._
  private val id = currentId.incrementAndGet()
  trace_(s"Create $id with base data ID $baseDataId.")

  private val path = tempDir.resolve(s"$id")
  private val cache = new CombinedCache(available, path, initialSize)

  def written: Boolean = synchronized(cache.written)
  def size: Long = synchronized(cache.size)

  /** @param readUnderlying (dataId, offset, size, sink)
    * @return `false` if request exceeds available size. */
  def read(offset: Long, size: Int, sink: Pointer, readUnderlying: (Long, Long, Int, Pointer) => Unit): Boolean = synchronized {
    ???
//    if (size > _size - offset) {
//      warn_(s"Requested size $size larger than available bytes between $offset and ${_size}.")
//      false
//    } else {
//      var position = offset
//      var parts = stored.filter { case (_, to) => to > position }
//      while(position < size) {
//        val (from, to) = parts.head
//        parts = parts.tail
//
//      }
//      ???
//    }
//    for (p <- position until position + sizeToRead by memChunk; chunkSize = math.min(memChunk, size - position).toInt) {
//      yield 1
//    }
  }

  def truncate(size: Long): Unit = synchronized { cache.truncate(size) }

  def write(offset: Long, size: Long, source: Pointer): Unit = synchronized {
    for (position <- 0L until size by memChunk; chunkSize = math.min(memChunk, size - position).toInt) {
      val chunk = new Array[Byte](chunkSize)
      source.get(position, chunk, 0, chunkSize)
      cache.write(offset + position, chunk)
    }
  }

  override def close(): Unit = synchronized {
    cache.close()
    trace_(s"Close $id with base data ID $baseDataId.")
    closedEntries.incrementAndGet()
  }
}
