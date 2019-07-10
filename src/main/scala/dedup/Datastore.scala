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
      .getOrElse(new RandomAccessFile(file, writeFlag).tap(_ => println(s"opened $file for writing")))
      .tap(openFiles.addOne(file, _))
  }

  def write(position: Long, data: Array[Byte]): Unit = {
    require(!readOnly, "Datastore is read-only.")
    val positionInFile = position % fileSize
    val bytesToWrite = math.min(fileSize - positionInFile, data.length).toInt
    val fileName = s"%010d".format((position / fileSize % 100) * fileSize) // 100MB per file
    val dir2 = f"${position / fileSize / 100 % 100}%02d"                   //  10GB per dir
    val dir1 = f"${position / fileSize / 100 / 100}%02d"                   //   1TB per dir
    println(s"write ${data.length} bytes at $position in $dir1 / $dir2 / $fileName : $positionInFile")
    val filePath = s"$basePath/$dir1/$dir2/$fileName"
    new File(filePath).getParentFile.mkdirs()
    open(filePath).tap(_.seek(positionInFile)).write(data)
    if (data.length > bytesToWrite) write(position + bytesToWrite, data.drop(bytesToWrite))
  }

  def read(position: Long, size: Int): Array[Byte] = {
    // FIXME duplicate code
    val positionInFile = position % fileSize
    val bytesToRead = math.min(fileSize - positionInFile, size).toInt
    val fileName = s"%010d".format((position / fileSize % 100) * fileSize) // 100MB per file
    val dir2 = f"${position / fileSize / 100 % 100}%02d"                   //  10GB per dir
    val dir1 = f"${position / fileSize / 100 / 100}%02d"                   //   1TB per dir
    println(s"read $size bytes from $position in $dir1 / $dir2 / $fileName : $positionInFile")
    val filePath = s"$basePath/$dir1/$dir2/$fileName"
    val bytes = new Array[Byte](size)
    open(filePath).pipe { ra =>
      ra.seek(positionInFile)
      ra.readFully(bytes) // TODO corrupt data files -> entry will not be read at all
    }
    if (size > bytesToRead) bytes ++ read(position + bytesToRead, size - bytesToRead)
    else bytes
  }

  override def close(): Unit = openFiles.values.foreach(_.close())
}
