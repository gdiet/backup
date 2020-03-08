package dedup.store

import java.io.{File, FileNotFoundException, RandomAccessFile}

import dedup.assertLogged
import org.slf4j.{Logger, LoggerFactory}

/** This class provides a sequential bytes store on disk limited by disc capacity only, accessed through its
 *  two main methods #read and #write. The byte store is thread safe. When used in a sequential write-once
 *  fashion, the #writeProtectCompleteFiles can be called to write protect data files that should not be
 *  touched anymore. The backing data files are filled up to 100.000.000 bytes, so they can be copied fast
 *  (not too many files) while being manageable on all file systems (not too large). When used in read-only
 *  mode, write access fails with an exception.
 */
class LongTermStore(dataDir: String, readOnly: Boolean) extends ParallelAccess[RandomAccessFile] {
  import LongTermStore._

  implicit private val log: Logger = LoggerFactory.getLogger(getClass)
  protected def openResource(path: String, forWrite: Boolean): RandomAccessFile = {
    log.debug(s"Open data file $path ${if (forWrite) "for writing" else "read-only"}")
    val file = new File(dataDir, path)
    if (forWrite) file.getParentFile.mkdirs()
    new RandomAccessFile(file, if (forWrite) "rw" else "r")
  }
  protected def closeResource(path: String, r: RandomAccessFile): Unit = { log.debug(s"Closed data file $path"); r.close() }

  def write(position: Long, data: Array[Byte]): Unit = {
    require(!readOnly, "Long term store is read-only, can't write.")
    val (path, offset, bytesToWrite) = pathOffsetSize(position, data.length)
    access(path, write = true) { file => file.seek(offset); file.write(data.take(bytesToWrite)) }
    if (data.length > bytesToWrite) write(position + bytesToWrite, data.drop(bytesToWrite))
  }

  def read(position: Long, size: Int): Array[Byte] = if (size == 0) Array() else {
    assertLogged(position >= 0, s"position >= 0 ... p = $position, s = $size")
    assertLogged(size > 0, s"size > 0 ... p = $position, s = $size")
    val (path, offset, bytesRequested) = pathOffsetSize(position, size)
    val bytes =
      try access(path, write = false)(file => new Array[Byte](bytesRequested).tap {
        val bytesToRead = math.min(bytesRequested, file.length - offset).toInt
        if (bytesToRead < bytesRequested)
          log.error(s"Data file $path too short, reading $bytesToRead of $bytesRequested from $offset")
        file.seek(offset); file.readFully(_, 0, bytesToRead)
      })
      catch { case e: FileNotFoundException =>
        log.error(s"Missing data file while trying to read $size bytes starting at $position", e)
        new Array[Byte](bytesRequested)
      }
    if (size > bytesRequested) bytes ++ read(position + bytesRequested, size - bytesRequested)
    else bytes
  }

  def writeProtectCompleteFiles(startPosition: Long, endPosition: Long): Unit = {
    for {position <- startPosition until endPosition-fileSize by fileSize} {
      val (path, _, _) = pathOffsetSize(position, 0)
      log.info(s"Write protecting $path")
      new File(s"$dataDir/$path").setReadOnly()
    }
  }
}
object LongTermStore {
  val fileSize = 100000000 // 100 MB, must be Int (not Long), don't change without migration script

  def pathOffsetSize(position: Long, size: Int): (String, Long, Int) = {
    val positionInFile = (position % fileSize).toInt     // 100 MB per file
    val dir2 = f"${position / fileSize / 100 % 100}%02d" //  10 GB per dir
    val dir1 = f"${position / fileSize / 100 / 100}%02d" //   1 TB per dir
    val path = f"$dir1%s/$dir2%s/${position - positionInFile}%010d"
    (path, positionInFile, math.min(fileSize - positionInFile, size))
  }
}
