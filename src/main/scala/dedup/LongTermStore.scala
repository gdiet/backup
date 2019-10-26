package dedup

import java.io.{File, RandomAccessFile}
import java.util.concurrent.Semaphore

import scala.collection.mutable

class LongTermStore(dataDir: String, readOnly: Boolean) extends AutoCloseable {
  private val fileSize = 100000000 // 100 MB
  private val parallelOpenFiles = 5

  private val openFiles: mutable.LinkedHashMap[String, (Semaphore, Boolean, RandomAccessFile)] = mutable.LinkedHashMap()
  private val mapSem = new Semaphore(1)

  private def access[T](path: String, write: Boolean)(f: RandomAccessFile => T): T = {
    mapSem.acquire()
    openFiles.get(path) match {
      case Some((fileSem, isForWrite, file)) =>
        fileSem.acquire()
        if (!write || isForWrite) { mapSem.release(); f(file).tap(_ => fileSem.release()) }
        else { file.close(); openFiles.remove(path); mapSem.release(); access(path, write)(f) }
      case None =>
        if (openFiles.size < parallelOpenFiles) {
          if (!readOnly) new File(path).getParentFile.mkdirs()
          val fileSem -> file = new Semaphore(0) -> new RandomAccessFile(path, if (readOnly || !write) "r" else "rw")
          openFiles.addOne(path, (fileSem, write, file))
          mapSem.release(); f(file).tap(_ => fileSem.release())
        } else {
          val (pathToClose, (_, _, file)) = openFiles
            .find { case (_, (sem, _, _)) =>  sem.tryAcquire() }
            .getOrElse { openFiles.head.tap { case (_, (sem, _, _)) => sem.acquire() } }
          file.close(); openFiles.remove(pathToClose)
          mapSem.release()
          access(path, write)(f)
        }
    }
  }
  
  override def close(): Unit = {
    mapSem.acquire()
    openFiles.values.foreach { case (fileSem, _, file) => fileSem.acquire(); file.close() }
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
    val (path, offset, bytesToWrite) = pathOffsetSize(position, data.length)
    access(path, write = true) { file => file.seek(offset); file.write(data.take(bytesToWrite)) }
    if (data.length > bytesToWrite) write(position + bytesToWrite, data.drop(bytesToWrite))
  }

  def read(position: Long, size: Int): Array[Byte] = {
    val (path, offset, bytesToRead) = pathOffsetSize(position, size)
    // Note: From corrupt data file, entry will not be read at all
    val bytes = access(path, write = false) { file => file.seek(offset); new Array[Byte](bytesToRead).tap(file.readFully) }
    if (size > bytesToRead) bytes ++ read(position + bytesToRead, size - bytesToRead)
    else bytes
  }
}
