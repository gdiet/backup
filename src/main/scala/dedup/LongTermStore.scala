package dedup

import java.io.{File, RandomAccessFile}

import scala.collection.mutable

class LongTermStore(dataDir: String, readOnly: Boolean) extends AutoCloseable {
  private val fileSize = 100000000 // 100 MB
  private val parallelOpenFiles = 3
  private val writeFlag = if (readOnly) "r" else "rw"

  private val openFiles: mutable.LinkedHashMap[String, RandomAccessFile] = mutable.LinkedHashMap()

  private def access[T](path: String)(f: RandomAccessFile => T): T = {
    if (openFiles.size >= parallelOpenFiles)
      openFiles.head match { case (f, _) => if (f != path) openFiles.remove(f).foreach(_.close()) }
    openFiles.remove(path).getOrElse {
      if (!readOnly) new File(path).getParentFile.mkdirs()
      new RandomAccessFile(path, writeFlag)
    }.tap(openFiles.addOne(path, _)).pipe(f)
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

  override def close(): Unit = openFiles.values.foreach(_.close())
}
