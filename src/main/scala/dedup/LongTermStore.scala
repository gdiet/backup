package dedup

import java.io.{File, RandomAccessFile}
import java.util.concurrent.locks.ReentrantLock

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/** This class provides a sequential bytes store on disk limited by disc capacity only, accessed through its
 *  two main methods #read and #write. The byte store is thread safe. When used in a sequential write-once
 *  fashion, the #writeProtectCompleteFiles can be called to write protect data files that should not be
 *  touched anymore. The backing data files are filled up to 100.000.000 bytes, so they can be copied fast
 *  (not too many files) while being manageable on all file systems (not too large). When used in read-only
 *  mode, write access fails with an exception.
 */
class LongTermStore(dataDir: String, readOnly: Boolean) extends AutoCloseable {
  implicit private val log: Logger = LoggerFactory.getLogger(getClass)
  private val fileSize = 100000000 // 100 MB
  private val parallelOpenFiles = 5

  private val openFiles: mutable.LinkedHashMap[String, (ReentrantLock, Boolean, RandomAccessFile)] = mutable.LinkedHashMap()
  private val mapLock = new ReentrantLock()

  @annotation.tailrec
  private def access[T](path: String, write: Boolean)(f: RandomAccessFile => T): T = {
    mapLock.lock()
    openFiles.get(path) match {
      case Some((fileLock, isForWrite, file)) =>
        fileLock.lock()
        if (!write || isForWrite) { mapLock.unlock(); f(file).tap(_ => fileLock.unlock()) }
        else { file.close(); openFiles.remove(path); mapLock.unlock(); access(path, write)(f) }
      case None =>
        if (openFiles.size < parallelOpenFiles) {
          log.debug(s"Open data file $path")
          if (!readOnly) new File(path).getParentFile.mkdirs()
          val fileLock -> file = new ReentrantLock() -> new RandomAccessFile(path, if (readOnly || !write) "r" else "rw")
          openFiles.addOne(path, (fileLock, write, file))
          fileLock.lock(); mapLock.unlock(); f(file).tap(_ => fileLock.unlock())
        } else {
          val (pathToClose, (_, _, file)) = openFiles
            .find { case (_, (fileLock, _, _)) =>  fileLock.tryLock() }
            .getOrElse { openFiles.head.tap { case (_, (fileLock, _, _)) => fileLock.lock() } }
          log.debug(s"Close data file $pathToClose")
          file.close(); openFiles.remove(pathToClose)
          mapLock.unlock()
          access(path, write)(f)
        }
    }
  }
  
  override def close(): Unit = {
    mapLock.lock()
    openFiles.values.foreach { case (fileLock, _, file) => fileLock.lock(); file.close() }
  }

  private def pathOffsetSize(position: Long, size: Int): (String, Long, Int) = {
    val positionInFile = position % fileSize
    val chunkSize = math.min(fileSize - positionInFile, size).toInt
    val fileName = s"%010d".format(position - positionInFile) // 100MB per file
    val dir2 = f"${position / fileSize / 100 % 100}%02d" //  10GB per dir
    val dir1 = f"${position / fileSize / 100 / 100}%02d" //   1TB per dir
    val filePath = s"$dataDir/$dir1/$dir2/$fileName"
    (filePath, positionInFile, chunkSize)
  }

  def write(position: Long, data: Array[Byte]): Unit = {
    require(!readOnly, "Long term store is read-only, can't write.")
    val (path, offset, bytesToWrite) = pathOffsetSize(position, data.length)
    access(path, write = true) { file => file.seek(offset); file.write(data.take(bytesToWrite)) }
    if (data.length > bytesToWrite) write(position + bytesToWrite, data.drop(bytesToWrite))
  }

  def read(position: Long, size: Int): Array[Byte] = if (size == 0) Array() else {
    assertLogged(position >= 0, s"position >= 0 ... p = $position, s = $size")
    assertLogged(size > 0, s"size > 0 ... p = $position, s = $size")
    val (path, offset, bytesToRead) = pathOffsetSize(position, size)
    // Note: From corrupt data file, entry will not be read at all
    val bytes = access(path, write = false) { file => file.seek(offset); new Array[Byte](bytesToRead).tap(file.readFully) }
    if (size > bytesToRead) bytes ++ read(position + bytesToRead, size - bytesToRead)
    else bytes
  }

  def writeProtectCompleteFiles(startPosition: Long, endPosition: Long): Unit = {
    for {position <- startPosition until endPosition-fileSize by fileSize} {
      val (path, _, _) = pathOffsetSize(position, 0)
      log.info(s"Write protecting $path")
      new File(path).setReadOnly()
    }
  }
}
