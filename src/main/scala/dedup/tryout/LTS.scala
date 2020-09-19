package dedup.tryout

import java.io.File
import java.nio.channels.SeekableByteChannel
import java.nio.file.{Files, Path}
import java.nio.file.StandardOpenOption.{CREATE_NEW, READ, SPARSE, WRITE}
import java.util.concurrent.atomic.AtomicLong

import dedup.scalaUtilChainingOps
import org.slf4j.{Logger, LoggerFactory}

object _2 {
  case class Part(start: Long, stop: Long) {
    assert(stop > start, s"stop $stop is not larger than start $start")
    assert(start >= 0, s"start $start is negative")
    def length: Long = stop - start
  }

  object CacheManager {
    val memoryUsed = new AtomicLong()
    var cacheLimit: Long = 100000000
  }

  object CacheEntry {
    def apply(position: Long, data: Array[Byte]): CacheEntry =
      new MemCacheEntry(position, data)
  }
  sealed trait CacheEntry {
    assert(position >= 0, s"negative position $position")
    assert(size > 0, s"size $size not positive at $position")
    def position: Long
    def size: Int
    def data: Array[Byte]
    protected def trimCheck(newSize: Int): Unit = {
      assert(newSize < size, s"newSize $newSize is not less than size $size")
      assert(newSize > 0, s"newSize $newSize is not greater than zero")
    }
    def trim(newSize: Int): CacheEntry
    def drop(): Unit
  }

  class MemCacheEntry(val position: Long, val data: Array[Byte]) extends CacheEntry {
    override def size: Int = data.length
    override def trim(newSize: Int): CacheEntry = {
      trimCheck(newSize)
      CacheManager.memoryUsed.addAndGet(newSize - size)
      new MemCacheEntry(position, data.take(newSize))
    }
    override def drop(): Unit = CacheManager.memoryUsed.addAndGet(-size)
  }

  object Util {
    def sectionOf(parts: Seq[Part], from: Long, to: Long): Seq[Part] = {
      require(to > from, s"to $to is not larger than from $from")
      require(from >= 0, s"from is negative: $from")
      val (_, result) = parts.foldLeft((0L, Vector.empty[Part])) {
        case ((position, result), _) if position >= to => (position, result)
        case ((position, result), part) if position + part.length <= from => (position + part.length, result)
        case ((position, result), part) =>
          val dropLeft = math.max(0, from - position)
          val dropRight = math.max(0, position + part.length - to)
          (position + part.length, result :+ part.copy(part.start + dropLeft, part.stop - dropRight))
      }
      result
    }
  }

  class Cache(dataId: Long, tempDir: Path, ltsParts: Seq[Part]) extends AutoCloseable {
    private var length: Long = ltsParts.map(_.length).sum
    private var ltsLength = length
    private var entries: Seq[CacheEntry] = Seq()
    private var maybeChannel: Option[SeekableByteChannel] = None

    private val log: Logger = LoggerFactory.getLogger(getClass)
    private def path = tempDir.resolve(dataId.toString)
    private def channel = synchronized {
      maybeChannel.getOrElse(
        Files.newByteChannel(path, WRITE, CREATE_NEW, SPARSE, READ).tap(c => maybeChannel = Some(c))
      )
    }

    def truncate(newLength: Long): Unit = synchronized {
      assert(newLength >= 0, s"negative newLength $newLength")
      // It might make sense to drop this assertion - let's see...
      assert(length != newLength, s"length $length equals newLength $newLength")
      if (length > newLength) {
        if (ltsLength > newLength) ltsLength = newLength
        val (keep, discard) = entries.partition(_.position < newLength)
        discard.foreach(_.drop())
        entries = keep.map {
          case e if e.position + e.size <= newLength => e
          case e => e.trim((newLength - e.position).toInt)
        }
      }
      length = newLength
    }

    def write(position: Long, data: Array[Byte]): Unit = synchronized {
      ???
    }

    private def readLtsOrZeros(start: Long, size: Int, readLts: Part => Array[Byte]): Array[Byte] = {
      if (start >= ltsLength) Array.fill[Byte](size)(0)
      else {
        ltsParts.foldLeft((0L, Array.empty[Byte])) {
          case ((posInLts, result), part) =>
            // FIXME continue
            (posInLts + part.length, result)
        }
      }
      ???
    }

    def read(position: Long, size: Int, readLts: Part => Array[Byte]): Array[Byte] = synchronized {
      assert(size > 0, s"size $size is not greater than zero")
      val dataFromEntries = entries.collect {
        case e if e.position > position && e.position < position + size =>
          val take = math.min(e.size, (position + size - e.position).toInt)
          e.position -> e.data.take(take)
        case e if e.position <= position && e.position + e.size > position =>
          val drop = (position - e.position).toInt
          val dropRight = math.max(0, (e.position + e.size - position - size).toInt)
          e.position + drop -> e.data.drop(drop).dropRight(dropRight)
      }.sortBy(_._1)
      val (currentPosition, result) = dataFromEntries.foldLeft((0L, Array.empty[Byte])) {
        case ((position, result), (entryPosition, entryData)) =>
          assert(entryPosition >= position, s"entryPosition $entryPosition less than position $position")
          val fromLts = readLtsOrZeros(position, (entryPosition - position).toInt, readLts)
          (entryPosition + entryData.length, result ++ fromLts ++ entryData)
      }
      result ++ readLtsOrZeros(currentPosition, (position - currentPosition).toInt, readLts)
    }

    override def close(): Unit = synchronized {
      maybeChannel.foreach { c =>
        c.close()
        log.debug(s"Deleting temporary store file for $dataId")
        Files.delete(path)
      }
      // invalidate this cache object
      maybeChannel = null
      entries = null
    }
  }
}

object _1 {
  trait LTS {
    def write(position: Long, data: Array[Byte]): Unit
    def read(position: Long, size: Int): Array[Byte]
  }

  case class Part(start: Long, stop: Long)

  case class Stored(dataId: Long, parts: Seq[Part])

  case class Cached(newDataId: Long, cache: Seq[CacheEntry], stored: Stored)

  object CacheManager {
    val memoryUsed = new AtomicLong()
  }
  /** Don't instantiate in read-only mode, not needed. */
  class CacheManager(tempPath: String) {
    private val tempDir: File = new File(tempPath, "dedupfs-temp")
    assert(tempDir.isDirectory || tempDir.mkdirs(), s"Can't create temp dir $tempDir")
    assert(tempDir.list().isEmpty, s"Temp dir is not empty: $tempDir")

  }

  sealed trait CacheEntry {
    def position: Long
    def data: Array[Byte]
    def length: Int
  }

  class MemoryEntry(val position: Long, val data: Array[Byte]) extends CacheEntry {
    override def toString: String = s"Mem($position+$length)"
    override def length: Int = data.length
  }

//  class FileEntry(channel: SeekableByteChannel, val position: Long, val length: Int) extends CacheEntry {
//    override def toString: String = s"Fil($position+$length)"
//    override def data: Array[Byte] = {
//      val buffer = java.nio.ByteBuffer.allocate(length)
//      val input = channel.position(position)
//      while(buffer.remaining() > 0) input.read(buffer)
//      new Array[Byte](length).tap(buffer.position(0).get)
//    }
//    override def memory: Long = 500
//    override def dropImpl(left: Int, right: Int): Entry =
//      copy(position = position + left, length = length - left - right)
//    override def writeImpl(offset: Int, data: Array[Byte]): Unit =
//      channel(id, dataId).position(position + offset).write(ByteBuffer.wrap(data))
//  }
}

