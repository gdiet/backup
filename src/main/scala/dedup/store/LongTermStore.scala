package dedup
package store

import java.io.{File, FileNotFoundException}
import java.util.concurrent.atomic.AtomicLong

def dataDir(repo: File) = File(repo, "data")

/** 100.000.000 bytes. Must be Int (not Long). Don't change without migration script. */
private val fileSize = 100000000

/** @param position The position in the store to access, must be >= 0.
  *                 Attempts to access position 9e18 or beyond yield an [[IllegalArgumentException]].
  * @param size     The number of bytes to access, must be >= 0.
  * @return The relative data file path, the offset in the data file,
  *         and the part of the requested size located on the data file. */
private def pathOffsetSize(position: Long, size: Long): (String, Long, Int) =
  ensure("lts.cap", 9000000000000000000L - position - size > 0, s"Exceeded storage cap: $position + $size")
  ensure("lts.position.negative", position >= 0, s"No sensible behavior implemented for negative position $position.")
  ensure("lts.size.negative", size >= 0, s"No sensible behavior implemented for negative size $size.")
  val positionInFile = (position % fileSize).toInt     // 100 MB per file
  val dir2 = f"${position / fileSize / 100 % 100}%02d" //  10 GB per dir
  val dir1 = f"${position / fileSize / 100 / 100}%02d" //   1 TB per dir
  val path = f"$dir1%s/$dir2%s/${position - positionInFile}%010d"
  (path, positionInFile, math.min(fileSize - positionInFile, size).toInt)

/** A sequential byte store on disk limited by disc capacity only, accessible by [[read]] and [[write]].
  * The byte store is thread safe. Backing data files are filled up to [[fileSize]] 100.000.000 bytes. This way
  * they can be copied fast (not too many files which slows down copy) while having a manageable in size for
  * all kinds of file systems. When used in read-only mode, write access fails with an exception. */
class LongTermStore(dataDir: File, readOnly: Boolean) extends ParallelAccess(dataDir):

  /** Unix time in millis when last a missing file was logged on WARN level. */
  private val missingFileLoggedLast = AtomicLong(now.asLong - 3600000) // Initialized as "one hour ago".

  /** @param position The position in the store to start writing at, must be >= 0.
    *                 Attempts to write at/beyond position 9e18 yield an [[IllegalArgumentException]].
    * @param data     The data to write to the store. */
  def write(position: Long, data: Array[Byte]): Unit =
    ensure("lts.protected", !readOnly, "Store is read-only, can't write.")
    ensure("lts.write.position", position >= 0, s"Write position $position must be >= 0.")
    val (path, offset, bytesToWrite) = pathOffsetSize(position, data.length)
    access(path, write = true) { file => file.seek(offset); file.write(data.take(bytesToWrite)) }
    if (data.length > bytesToWrite) write(position + bytesToWrite, data.drop(bytesToWrite))

  /** @param position The position in the store to start reading at, must be >= 0.
    *                 Attempts to read from/beyond position 9e18 yield an [[IllegalArgumentException]].
    * @param size     The number of bytes to read, must be >= 0.
    * @return (actual size, data), where actual size is capped by [[dedup.memChunk]]
    *         and by the number of bytes available in the data file to access. */
  private def readChunk(position: Long, size: Long): (Int, Array[Byte]) =
    ensure("lts.read.position", position >= 0, s"Read position $position must be >= 0.")
    ensure("lts.read.size", size >= 0, s"Read size $size must be >= 0.")
    val (path, offset, bytesFromFile) = pathOffsetSize(position, size)
    val bytesToReturn = math.min(dedup.memChunk, bytesFromFile)
    val bytes = new Array[Byte](bytesToReturn)
    try access(path, write = false) { file =>
      val bytesInFile = file.length - offset
      val bytesToRead = math.min(bytesToReturn, bytesInFile).toInt
      if (bytesToRead < bytesToReturn) log.warn(s"Data file $path too short, reading $bytesToRead of $bytesToReturn from $offset.")
      file.seek(offset)
      file.readFully(bytes, 0, bytesToRead)
    } catch { case _: FileNotFoundException =>
      if missingFileLoggedLast.get() + 300000 < now.asLong then // full log every five minutes
        missingFileLoggedLast.set(now.asLong)
        log.warn(s"Missing data file $path while reading at $position, substituting with '0' values.")
      else
        log.debug(s"Missing data file $path while reading at $position, substituting with '0' values.")
    }
    bytesToReturn -> bytes

  /** @param position     The position in the store to start reading at, must be >= 0.
    *                     Attempts to read from/beyond position 9e18 yield an [[IllegalArgumentException]].
    * @param size         The number of bytes to read, must be >= 0.
    * @param resultOffset Start offset for return positions.
    * @return A contiguous LazyList(position, bytes) where data chunk size is limited to [[dedup.memChunk]]. */
  def read(position: Long, size: Long, resultOffset: Long): LazyList[(Long, Array[Byte])] =
    if size == 0 then LazyList() else
      val (alreadyRead, bytes) = readChunk(position, size)
      if alreadyRead == size then LazyList(resultOffset -> bytes)
      else (resultOffset -> bytes) #:: read(position + alreadyRead, size - alreadyRead, resultOffset + alreadyRead)
