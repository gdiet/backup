package dedup
package server

class Level1(settings: Settings) extends AutoCloseable with util.ClassLogging {

  def entry(path: String): Option[TreeEntry] = ???
  def size(id: Long, dataId: Long): Long = ???
  override def close(): Unit = ???
  
}
