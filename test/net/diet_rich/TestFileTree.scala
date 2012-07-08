package net.diet_rich

import java.io.InputStreamReader
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import scala.io.Source

class TreeEntry(val name: String, val timeAndSize: Option[(Long, Long)] = None, var children: List[TreeEntry] = Nil) {
  def time = timeAndSize.get._1
  def size = timeAndSize.get._2
  def printAllWithPrefix(prefix: String) : Unit = {
    println(prefix + name)
    children.reverse.foreach(_ printAllWithPrefix(prefix + name + "/"))
  }
}

object TestFileTree {

  def treeRoot: TreeEntry = {
    val root = new TreeEntry("");
    val prefixMap = collection.mutable.Map[String, TreeEntry]()
    prefixMap.put("/", root)
  
    val in = new GZIPInputStream(new FileInputStream("test/filetree.gz"))
    val source = Source.fromInputStream(in, "UTF-8")
  
    source.getLines.foreach { line =>
      val isDir = line.startsWith("D")
      val prefix = line.substring(1, line.lastIndexOf("/")+1)
      val entry  = line.substring(line.lastIndexOf("/")+1)
      
      if (isDir) {
        val newEntry = new TreeEntry(entry)
        val x = prefixMap.get(prefix).get.children = newEntry :: prefixMap.get(prefix).get.children
        prefixMap.put(prefix+"/", newEntry)
      } else {
        val parts = entry.split(":")
        val name = parts(0)
        val time = parts(1).toLong
        val size = parts(2).toLong
        val newEntry = new TreeEntry(name, Some(time, size))
        val x = prefixMap.get(prefix).get.children = newEntry :: prefixMap.get(prefix).get.children
      }
    }
    
    source.close
    root
  }
  
}