package dedup
package cache

import java.nio.file.{Files, Path}
import dedup.cache.CacheBase
import dedup.util.ClassLogging

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.StandardOpenOption._

/** Caches in a file byte arrays with positions, where the byte arrays are not necessarily contiguous.
  * For performance uses a sparse file channel. Read and write methods are synchronized because 
  * otherwise concurrent changes to the channel position may occur. */
class FileCache(path: Path) extends LongCache with AutoCloseable with ClassLogging:

  private var maybeChannel: Option[SeekableByteChannel] = None
  private def channel = maybeChannel.getOrElse(
    Files.newByteChannel(path, WRITE, CREATE_NEW, SPARSE, READ).tap(c => maybeChannel = Some(c))
  )

  def write(position: Long, data: Array[Byte]): Unit = synchronized {
    clear(position, data.length)
    channel.position(position)
    channel.write(ByteBuffer.wrap(data, 0, data.length))
    entries.put(position, data.length)
    mergeIfPossible(position)
  }

  /** Reads cached byte areas from this [[FileCache]].
    *
    * @param position position to start reading at.
    * @param size     number of bytes to read.
    * @return An Iterator of (position, gapSize | byte array]). */
  def readData(position: Long, size: Long): Iterator[(Long, Either[Long, Array[Byte]])] = synchronized {
    read(position, size).flatMap {
      case entryPos -> Right(entrySize) =>
        val end = entryPos + entrySize
        Iterator.range(entryPos, end, memChunk.toLong).map { localPos =>
          val localSize = math.min(end - localPos, memChunk).asInt
          val bytes = new Array[Byte](localSize)
          val buffer = ByteBuffer.wrap(bytes)
          channel.position(localPos)
          if channel.read(buffer) < localSize then while (channel.read(buffer) > 0) {/**/}
          localPos -> Right(bytes)
        }
      case position -> Left(hole) => Seq(position -> Left(hole))
    }
  }

  override def close(): Unit = synchronized {
    maybeChannel.foreach { file =>
      file.close()
      Files.delete(path)
      log.debug(s"Closed & deleted cache file $path")
    }
    maybeChannel = None
  }
