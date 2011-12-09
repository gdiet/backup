package net.diet_rich.backup.fs

object TryApp extends App {

  import java.io.File
  
  val base = new File("E:/georg/bin/eclipse")
  
  val sqldb = new DedupSqlDb
  val db = new DedupDb(sqldb)
  val fs = new DedupFileSystem(db)

  def insert(path: String, name: String) : Unit = {
    val parent = fs.path(path).file.get
    parent.mkChild(name)
  }
  
  def listRecurse(path: String, file: File) : Unit = {
//    println(path + "/" + file.getName)
    insert(path, file.getName)
    val children = file.listFiles
    if (children != null) children.foreach(child => listRecurse(path + "/" + file.getName, child))
  }
  
  var time = System.currentTimeMillis()
  
  def createRecurse(path: String, depth: Int) : Unit = {
    if (depth == 1) {
      println(System.currentTimeMillis() - time)
      time = System.currentTimeMillis()
    }
    val filename = "b" + math.random
    insert (path, filename)
    if (depth < 4)
      for (i <- 0 until 10)
        createRecurse(path+"/"+filename, depth+1)
  }

  def createRecurse2(path: String, depth: Int) : Unit = {
    if (depth == 1) {
      println(System.currentTimeMillis() - time)
      time = System.currentTimeMillis()
    }
    val filename = "a" + math.random
    insert (path, filename)
    if (depth < 4)
      for (i <- 0 until 10)
        createRecurse(path+"/"+filename, depth+1)
  }

//  val time = System.currentTimeMillis()
//  listRecurse("", base)
  createRecurse("", 0)
//  createRecurse2("", 0)
  println(System.currentTimeMillis() - time)

  // 29.557 files 6522 folders - 8120 ms to listRecurse
  // 11136 ms to insert
 
  // >100.000 entries
  // 340 ms createRecurse
  // 22500 ms to insert
  // -> ~4000 entries per second
  
  
  
  def a {
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
}
