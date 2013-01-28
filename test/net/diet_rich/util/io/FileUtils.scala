package net.diet_rich.util.io

import java.io.File

object FileUtils {
  def iterateFiles(file: File, namePattern: String): Iterator[File] =
    if (file.isDirectory()) {
      file.listFiles().toIterator.flatMap(iterateFiles(_, namePattern))
    } else {
      if (file.getName().matches(namePattern)) Iterator(file) else Iterator()
    }
}