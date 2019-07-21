package dedup

import java.io.File

object Step0_Clear extends App {
  def delete(file: File): Unit = {
    if (file.isDirectory) file.listFiles.foreach(delete)
    file.delete()
  }
  delete(new File("fsdb"))
  delete(new File("data"))
}
