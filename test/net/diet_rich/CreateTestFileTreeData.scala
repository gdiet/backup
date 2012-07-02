package net.diet_rich

import java.io.File
import java.util.zip.GZIPOutputStream
import java.io.FileOutputStream
import java.io.OutputStreamWriter

object CreateTestFileTreeData extends App {

  val base = "d:\\bin\\eclipse"

  val out = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream("test/filetree.gz")), "UTF-8")
    
  def recurse(file: File, prefix: String) {
    if (file.isDirectory)
      out.write("D" + prefix + file.getName)
    else
      out.write("F" + prefix + file.getName + ":" + file.lastModified + ":" + file.length)
    out.write("\n")
    if (file.isDirectory) file.listFiles.foreach(recurse(_, prefix+"/"))
  }

  recurse(new File(base), "/")
  
  out.close
}