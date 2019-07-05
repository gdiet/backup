package dedup

trait FSInterface extends AutoCloseable {
  def entryAt(path: String): Option[FSEntry]
}

sealed trait FSEntry

trait FSFile extends FSEntry {
  def bytes(offset: Long, size: Int): Array[Byte]
  def size: Long
  def lastModifiedMillis: Long
}

trait FSDir extends FSEntry {
  def childNames: Seq[String]
}
