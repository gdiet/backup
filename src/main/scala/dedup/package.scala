package object dedup {

  def lookup(path: String, db: Database): Option[Database.ByParentNameResult] =
    path.split("/").filter(_.nonEmpty).foldLeft(Option(Database.byParentNameRoot)) {
      case (parent, name) => parent.flatMap(p => db.entryByParentAndName(p.id, name))
    }

}
