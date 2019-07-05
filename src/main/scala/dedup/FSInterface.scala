package dedup

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
