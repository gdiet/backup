package dedup
package server

class Level1(settings: Settings) extends AutoCloseable with util.ClassLogging {
  val backend = Level2(settings)
  export backend.{child, close, setTime}

  def split(path: String)       : Array[String]     = path.split("/").filter(_.nonEmpty)
  def entry(path: String)       : Option[TreeEntry] = entry(split(path))
  def entry(path: Array[String]): Option[TreeEntry] = path.foldLeft(Option[TreeEntry](root)) {
                                                        case (Some(dir: DirEntry), name) => child(dir.id, name)
                                                        case _ => None
                                                      }

  def size(id: Long, dataId: Long): Long = 0 // FIXME
}
