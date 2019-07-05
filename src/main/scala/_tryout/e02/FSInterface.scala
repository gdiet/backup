package _tryout.e02

trait FSInterface {
  def entryAt(path: String): Option[FSEntry]
}

sealed trait FSEntry

trait FSFile extends FSEntry {
  def bytes(offset: Long, size: Int): Array[Byte]
  def size: Long
  def lastModifiedMillis: Long = System.currentTimeMillis - 1000*3600*24*3
}

trait FSDir extends FSEntry {
  def childNames: Seq[String]
}

object MemFS extends FSInterface {
  private object root extends FSDir {
    override def childNames: Seq[String] = Seq("file.txt", "directory")
  }
  private object directory extends FSDir {
    override def childNames: Seq[String] = (0 to 9999).map(n => f"$n%04d.txt")
  }
  private def file(path: String): FSFile = new FSFile {
    private val content = s"file content of $path".getBytes("UTF-8")
    override def size: Long = content.length
    override def bytes(offset: Long, size: Int): Array[Byte] =
      content.drop(offset.toInt).take(size)
  }
  private val filePattern = "/directory/(\\d\\d\\d\\d).txt".r
  override def entryAt(path: String): Option[FSEntry] =
    path match {
      case "/" => Some(root)
      case "/directory" => Some(directory)
      case "/file.txt" | filePattern(_) => Some(file(path))
      case _ => None
    }
}
