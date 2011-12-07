package net.diet_rich.backup.fs

object TryApp extends App {

  val db = new DedupSqlDb
  val fs = new DedupFileSystem(db)
  println(fs.path("/a/b").parent)
  println(fs.path("/a").parent)
  println(fs.path("/a").file)
  println(fs.path("").file.map(_.mkChild("a")))

  println("---")

  println(fs.path("/a/b").parent)
  println(fs.path("/a").parent)
  println(fs.path("/a").file)
  println(fs.path("").file.flatMap(_.mkChild("a")))
}