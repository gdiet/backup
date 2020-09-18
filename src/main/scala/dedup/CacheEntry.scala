package dedup

import java.util.concurrent.atomic.AtomicLong

object CacheManager {
  val memoryUsed = new AtomicLong()
  var cacheLimit: Long = 100000000
}

class CacheEntry(ltsParts: Parts) {
  var size: Long = 0
  // TODO: when implementing, limit byte arrays to memChunk
  /** @param dataSource A function (offset, size) => data that provides the data to write. */
  def write(offset: Long, intSize: Int, dataSource: (Int, Int) => Array[Byte]): Unit = ???
  def truncate(size: Long): Unit = ???
  def dataWritten: Boolean = ???
  /** @param ltsRead A function (offset, size) => data that provides data from the long term store. */
  def read(start: Long, size: Long, ltsRead: (Long, Int) => Array[Byte]): LazyList[Array[Byte]] = ???
  /*
            val end = math.min(cacheEntry.size, offset + size)
            (for (position <- offset until end by memChunk; chunkSize = math.min(memChunk, end - position).toInt) yield {
              val bytes = cacheEntry.read_old(position, chunkSize, ???)
              buf.put(position - offset, bytes, 0, bytes.length)
              chunkSize
            }).sum

   */
  /*
            val end = offset + size
            (for (position <- offset until end by memChunk; chunkSize = math.min(memChunk, end - position).toInt) yield {
              val data = new Array[Byte](chunkSize)
              buf.get(position - offset, data, 0, chunkSize)
              cacheEntry.write(position, data)

            }).sum

            val data = new Array[Byte](intSize)
            store.write(file.id, file.dataId, db.dataSize(file.dataId))(offset, data)
            intSize

   */
  def drop(): Unit = ???
}
