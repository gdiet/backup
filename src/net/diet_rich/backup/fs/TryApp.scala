package net.diet_rich.backup.fs

import collection.JavaConversions.asScalaIterator
import java.io.File
import org.apache.commons.io.FileUtils

object TryApp extends App {

  val sqldb = new DedupSqlDb
  val db = new DedupDb(sqldb)
  val fs = new DedupFileSystem(db)

  val base = new File("C:/bin/eclipse")
  val baseLength = base.getPath length
  val files = FileUtils iterateFiles(base, null, true)
  
  files.foreach(file => {
    val path = file.getPath substring(baseLength) replace("\\","/")
    val entry = fs.path(path).getOrMakeEntry
    if (file.isFile())
      entry.setFileData(file.lastModified(), 0)
  })
  
}
