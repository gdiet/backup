package dedup
package store

import dedup.util.ClassLogging

import java.io.{File, RandomAccessFile}
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable
import scala.util.Try
import dedup.store.ParallelAccess

/** Manages thread safe parallel access to dedupfs data files so that only a limited
  * number of files is kept open and that write access is only used where necessary. */
trait ParallelAccess(dataDir: File) extends AutoCloseable with ClassLogging:

  private val parallelOpenFiles = 5
  private val mapLock = ReentrantLock()
  private val openFiles = mutable.LinkedHashMap[String, (ReentrantLock, Boolean, RandomAccessFile)]()

  /** @throws java.io.FileNotFoundException when trying read access to a non-existing data file. */
  private def openFile(path: String, forWrite: Boolean): RandomAccessFile =
    log.debug(s"Opening data file $path ${if forWrite then "for writing" else "read-only"}")
    val file = File(dataDir, path)
    if (forWrite) file.getParentFile.mkdirs()
    RandomAccessFile(file, if forWrite then "rw" else "r")

  private def closeFile(path: String, r: RandomAccessFile): Unit =
    r.close()
    log.debug(s"Closed data file $path")

  /** Read access to data files should be possible even if they are read-only, e.g. because the user doesn't have write
    * access to the repository.
    *
    * Read access attempts to missing data files must not cause changes in the file system, i.e. must not create any
    * missing parent directories or the data file itself.
    *
    * @throws java.io.FileNotFoundException when trying read access to a non-existing data file. */
  @annotation.tailrec
  final def access(path: String, write: Boolean)(f: RandomAccessFile => _): Unit = {
    mapLock.lock()
    openFiles.get(path) match

      case Some(entry@(fileLock, isForWrite, file)) => // entry found for path
        fileLock.lock()
        if !write || isForWrite then // can use open file
          openFiles.remove(path)
          openFiles.put(path, entry) // remove and add for LRU functionality
          mapLock.unlock()
          try f(file) finally fileLock.unlock()
        else // need to re-open r/w
          closeFile(path, file)
          openFiles.remove(path)
          mapLock.unlock()
          access(path, write)(f)

      case None if openFiles.size < parallelOpenFiles => // create new entry for path
        val file = try openFile(path, write) catch { case t: Throwable => mapLock.unlock(); throw t }
        val fileLock = ReentrantLock().tap(_.lock())
        openFiles.put(path, (fileLock, write, file))
        mapLock.unlock()
        try f(file) finally fileLock.unlock()

      case None => // limit reached - close a file then try again
        // This approach is better than simply using java.util.LinkedHashMap with removeEldestEntry
        // because it first looks for a file that can be closed immediately. Only if unavailable,
        // it waits for the eldest file to become available. Note that in this case it all subsequent
        // access is locked until that eldest file has been closed.
        val (pathToClose, (_, _, resource)) = openFiles
          .find { case (_, (fileLock, _, _)) => fileLock.tryLock() }
          .getOrElse { openFiles.head.tap { case (_, (fileLock, _, _)) => fileLock.lock() } }
        closeFile(pathToClose, resource)
        openFiles.remove(pathToClose)
        mapLock.unlock()
        access(path, write)(f)
  }

  def close(): Unit =
    mapLock.lock()
    openFiles.foreach { case (path, (fileLock, _, resource)) =>
      fileLock.lock()
      closeFile(path, resource)
    }
