package net.diet_rich.backup.dedupfs

object Tryout extends App {

  val db = new CachedDB(new MemoryDB())
  val fs = new DedupFS(db)

//  fs.get("")
  println(fs.get("/a").mkdir)
//  println(fs.get("/a/b").mkdir)
  
  println(fs.get("/a").data)
  
  // get root
  // mkdir a
  // mkdirs a/b/c
  // get a/b
  
}