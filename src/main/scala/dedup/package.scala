package object dedup {

  def split(path: String): Array[String] = {
    require(path.startsWith("/"), s"Path does not start with '/': $path")
    path.split("/").filter(_.nonEmpty)
  }

  def lookup(path: String, db: Database): Option[Database.ByParentNameResult] =
    split(path).foldLeft(Option(Database.byParentNameRoot)) {
      case (parent, name) => parent.flatMap(p => db.entryByParentAndName(p.id, name))
    }

}
