package dedup

import java.io.File

object CleanInit extends App {
  def delete(file: File): Unit = {
    if (file.isDirectory) file.listFiles.foreach(delete)
    file.delete()
  }
  delete(new File("fsdb"))
  delete(new File("data"))
  delete(new File("dedupfs-temp"))
  Server.main(Array("init"))
}
