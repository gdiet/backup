package dedup.store

import dedup.memChunk

import java.io.{File, FileNotFoundException, RandomAccessFile}
import java.lang.System.{currentTimeMillis => now}
import java.util.concurrent.atomic.AtomicLong
import org.slf4j.{Logger, LoggerFactory}

/** This class provides a sequential bytes store on disk limited by disc capacity only, accessed through its
  *  two main methods #read and #write. The byte store is thread safe. When used in a sequential write-once
  *  fashion, the #writeProtectCompleteFiles can be called to write protect data files that should not be
  *  touched anymore. The backing data files are filled up to 100.000.000 bytes, so they can be copied fast
  *  (not too many files) while being manageable on all file systems (not too large). When used in read-only
  *  mode, write access fails with an exception.
  */
class LongTermStore(dataDir: File, readOnly: Boolean) extends ParallelAccess[RandomAccessFile] {
  import LongTermStore._

  implicit private val log: Logger = LoggerFactory.getLogger("dedup.Store")
  private val missingFileLoggedLast = new AtomicLong(now - 3600000) // One hour ago

  protected def openResource(path: String, forWrite: Boolean): RandomAccessFile = {
    log.debug(s"Open data file $path ${if (forWrite) "for writing" else "read-only"}")
    val file = new File(dataDir, path)
    if (forWrite) file.getParentFile.mkdirs()
    new RandomAccessFile(file, if (forWrite) "rw" else "r")
  }
  protected def closeResource(path: String, r: RandomAccessFile): Unit = {
    r.close()
    log.debug(s"Closed data file $path")
  }

  def write(position: Long, data: Array[Byte]): Unit = {
    require(!readOnly, "Long term store is read-only, can't write.")
    val (path, offset, bytesToWrite) = pathOffsetSize(position, data.length)
    access(path, write = true) { file => file.seek(offset); file.write(data.take(bytesToWrite)) }
    if (data.length > bytesToWrite) write(position + bytesToWrite, data.drop(bytesToWrite))
  }

  /** @return (actual size, data), where actual size is capped by the internal size limit for byte arrays
    *         and by the number of bytes available in the first data file to access. */
  private def readChunk(position: Long, size: Long): (Int, Array[Byte]) = {
    assert(position >= 0, s"position >= 0 ... position: $position, size: $size")
    assert(size > 0, s"size > 0 ... position: $position, size: $size")
    val (path, offset, bytesFromFile) = pathOffsetSize(position, size)
    val bytesToReturn = math.min(memChunk, bytesFromFile)
    val bytes = new Array[Byte](bytesToReturn)
    try access(path, write = false) { file =>
      val bytesInFile = file.length - offset
      val bytesToRead = math.min(bytesToReturn, bytesInFile).toInt
      if (bytesToRead < bytesToReturn) log.error(s"Data file $path too short, reading $bytesToRead of $bytesToReturn from $offset")
      file.seek(offset); file.readFully(bytes, 0, bytesToRead)
    } catch { case e: FileNotFoundException =>
      if (missingFileLoggedLast.get() + 300000 < now) { // full log every five minutes
        missingFileLoggedLast.set(now)
        log.warn(s"Missing data file while reading at $position, substituting with '0' values.")
        log.debug(s"Missing data file - full stack trace:", e)
      } else log.debug(s"Missing data file while reading at $position, substituting with '0' values.")
    }
    bytesFromFile -> bytes
  }

  /** @param size Not limited to internal size limit for byte arrays.
    * @param resultOffset Offset to start return positions at.
    * @return A contiguous LazyList(offset, data) where data chunk size
    *         is limited to internal size limit for byte arrays. */
  def read(position: Long, size: Long, resultOffset: Long): LazyList[(Long, Array[Byte])] = if (size == 0) LazyList() else {
    val (alreadyRead, bytes) = readChunk(position, size)
    if (alreadyRead == size) LazyList(resultOffset -> bytes)
    else (resultOffset -> bytes) #:: read(position + alreadyRead, size - alreadyRead, resultOffset + alreadyRead)
  }
}

object LongTermStore {
  def ltsDir(repo: File): File = new File(repo, "data").tap { d => d.mkdirs(); require(d.isDirectory) }

  private[store] val fileSize = 100000000 // 100 MB, must be Int (not Long), don't change without migration script

  /** @return The relative data file path, the offset in the data file,
    *         and the part of the requested size located on the data file. */
  private[store] def pathOffsetSize(position: Long, size: Long): (String, Long, Int) = {
    val positionInFile = (position % fileSize).toInt     // 100 MB per file
    val dir2 = f"${position / fileSize / 100 % 100}%02d" //  10 GB per dir
    val dir1 = f"${position / fileSize / 100 / 100}%02d" //   1 TB per dir
    val path = f"$dir1%s/$dir2%s/${position - positionInFile}%010d"
    (path, positionInFile, math.min(fileSize - positionInFile, size).toInt)
  }
}
