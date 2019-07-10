package dedup

import java.io.{File, RandomAccessFile}

import scala.collection.mutable

object Datastore {
  def datastoreDir(repo: File): File = new File(repo, "data")
  val hashAlgorithm = "SHA-1"
}

class Datastore(baseDir: File, readOnly: Boolean) extends AutoCloseable {
  private val fileSize = 100000000
  private val parallelOpenFiles = 5
  private val basePath = baseDir.getAbsolutePath
  private val writeFlag = if (readOnly) "r" else "rw"

  private val openFiles: mutable.LinkedHashMap[String, RandomAccessFile] = mutable.LinkedHashMap()

  private def open(file: String): RandomAccessFile = {
    if (openFiles.size >= parallelOpenFiles)
      openFiles.head match { case (f, _) => if (f != file) openFiles.remove(f).foreach(_.close()) }
    openFiles.remove(file)
      .getOrElse {
        if (!readOnly) new File(file).getParentFile.mkdirs()
        new RandomAccessFile(file, writeFlag).tap(_ => println(s"opened $file for writing"))
      }
      .tap(openFiles.addOne(file, _))
  }

  private def pathOffsetSize(position: Long, size: Int): (String, Long, Int) = {
    def positionInFile = position % fileSize
    val chunkSize = math.min(fileSize - positionInFile, size).toInt
    val fileName = s"%010d".format((position / fileSize % 100) * fileSize) // 100MB per file
    val dir2 = f"${position / fileSize / 100 % 100}%02d"                   //  10GB per dir
    val dir1 = f"${position / fileSize / 100 / 100}%02d"                   //   1TB per dir
    val filePath = s"$basePath/$dir1/$dir2/$fileName"
    (filePath, positionInFile, chunkSize)
  }

  def write(position: Long, data: Array[Byte]): Unit = {
    val (path, offset, bytesToWrite) = pathOffsetSize(position, data.length)
    println(s"write ${data.length} bytes at position $position in offset $offset in $path")
    open(path).tap(_.seek(offset)).write(data)
    if (data.length > bytesToWrite) write(position + bytesToWrite, data.drop(bytesToWrite))
  }

  def read(position: Long, size: Int): Array[Byte] = {
    val (path, offset, bytesToRead) = pathOffsetSize(position, size)
    println(s"read $bytesToRead bytes from $position in offset $offset in $path")
    val bytes = new Array[Byte](bytesToRead)
    open(path).pipe { ra =>
      ra.seek(offset)
      ra.readFully(bytes) // TODO corrupt data files -> entry will not be read at all
    }
    if (size > bytesToRead) bytes ++ read(position + bytesToRead, size - bytesToRead)
    else bytes
  }

  override def close(): Unit = openFiles.values.foreach(_.close())
}
