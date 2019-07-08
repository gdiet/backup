package dedup

import java.io.File

object Datastore {
  def datastoreDir(repo: File): File = new File(repo, "data")
  val hashAlgorithm = "SHA-1"
}

class Datastore(baseDir: File) extends AutoCloseable {
  private val fileSize = 100000000

  def write(position: Long, data: Array[Byte]): Unit = {
    val positionInFile = position % fileSize
    val bytesToWrite = math.min(fileSize - positionInFile, data.length).toInt
    val fileName = s"%010d".format((position / fileSize % 100) * fileSize) // 100MB per file
    val dir2 = f"${position / fileSize / 100 % 100}%02d"                   //  10GB per dir
    val dir1 = f"${position / fileSize / 100 / 100}%02d"                   //   1TB per dir
    println(s"write ${data.length} bytes at $position in $dir1 / $dir2 / $fileName : $positionInFile")
    if (data.length > bytesToWrite) write(position + bytesToWrite, data.drop(bytesToWrite))
  }
  def read(position: Long, size: Int): Array[Byte] = {
    val positionInFile = position % fileSize
    val bytesToRead = math.min(fileSize - positionInFile, size).toInt
    val fileName = s"%010d".format((position / fileSize % 100) * fileSize) // 100MB per file
    val dir2 = f"${position / fileSize / 100 % 100}%02d"                   //  10GB per dir
    val dir1 = f"${position / fileSize / 100 / 100}%02d"                   //   1TB per dir
    println(s"read $size bytes from $position in $dir1 / $dir2 / $fileName : $positionInFile")
    val bytes = Array.fill[Byte](size)(65)
    if (size > bytesToRead) bytes ++ read(position + bytesToRead, size - bytesToRead)
    else bytes
  }
  override def close(): Unit = ()
}
