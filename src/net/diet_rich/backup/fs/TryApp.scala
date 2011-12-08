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
  println(fs.path("/a/b").file.flatMap(_.mkChild("x")))
  println(fs.path("/a/b").file.flatMap(_.path))
  
  println("---")

  println(fs.path("/a").file.map(_.rename("c")))
  println(fs.path("/a/b").file)
  println(fs.path("/c/b").file)
  
  println("---")

  println(fs.path("/c").file.map(_.delete))
  println(fs.path("/c/b").file)
  
  Thread sleep 500
}
