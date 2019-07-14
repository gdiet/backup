package object dedup extends scala.util.ChainingSyntax {

  def sync[T](f: => T): T = synchronized(f)

  def split(path: String): Array[String] = path.split("/").filter(_.nonEmpty)

  def lookup(path: String, db: Database): Option[Database.TreeNode] =
    split(path).foldLeft(Option(Database.root)) {
      case (parent, name) => parent.flatMap(p => db.child(p.id, name))
    }

  def globPattern(glob: String): String =
    s"\\Q${glob.replaceAll("\\*", "\\\\E.*\\\\Q").replaceAll("\\?", "\\\\E.\\\\Q")}\\E"
}
