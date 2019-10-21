package dedup

import java.io.RandomAccessFile
import java.util.concurrent.Semaphore

import scala.collection.mutable

class SynchonizedLongTermStore(dataDir: String, readOnly: Boolean) extends AutoCloseable {
  private val fileSize = 100000000 // 100 MB
  private val parallelOpenFiles = 3
  private val writeFlag = if (readOnly) "r" else "rw"

  private val openFiles: mutable.LinkedHashMap[String, (Semaphore, RandomAccessFile)] = mutable.LinkedHashMap()
  private val mapSem = new Semaphore(1)

  private def access[T](path: String)(f: RandomAccessFile => T): T = {
    mapSem.acquire()
    openFiles.get(path) match {
      case Some(fileSem -> file) =>
        fileSem.acquire(); mapSem.release(); f(file).tap(_ => fileSem.release())
      case None =>
        if (openFiles.size < parallelOpenFiles) {
          val fileSem -> file = new Semaphore(0) -> new RandomAccessFile(path, writeFlag)
          openFiles.addOne(path, fileSem -> file)
          mapSem.release(); f(file).tap(_ => fileSem.release())
        } else {
          val (pathToClose, _ -> file) = openFiles
            .find { case (_, sem -> _) =>  sem.tryAcquire() }
            .getOrElse { openFiles.head.tap { case (_, sem -> _) => sem.acquire() } }
          file.close(); openFiles.remove(pathToClose)
          mapSem.release()
          access(path)(f)
        }
    }
  }
  
  override def close(): Unit = {
    mapSem.acquire()
    openFiles.values.foreach { case fileSem -> file => fileSem.acquire(); file.close() }
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
    access(path) { file => file.seek(offset); file.write(data.take(bytesToWrite)) }
    if (data.length > bytesToWrite) write(position + bytesToWrite, data.drop(bytesToWrite))
  }

  def read(position: Long, size: Int): Array[Byte] = {
    val (path, offset, bytesToRead) = pathOffsetSize(position, size)
    // Note: From corrupt data file, entry will not be read at all
    val bytes = access(path) { file => file.seek(offset); new Array[Byte](bytesToRead).tap(file.readFully) }
    if (size > bytesToRead) bytes ++ read(position + bytesToRead, size - bytesToRead)
    else bytes
  }
}
