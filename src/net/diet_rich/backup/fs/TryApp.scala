package net.diet_rich.backup.fs

object TryApp extends App {

  val sqldb = new DedupSqlDb
  val db = new DedupDb(sqldb)
  val fs = new DedupFileSystem(db)
  println(fs.path("/a").file)
  println(fs.path("").file.map(_.mkChild("a")))

  println("---")

  println(fs.path("/a").file)
  println(fs.path("").file.flatMap(_.mkChild("a")))
  
  println("---")

  println(fs.path("/a").file.flatMap(_.mkChild("b")))
  println(fs.path("/a/b").file.flatMap(_.path))
}
