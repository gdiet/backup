package dedup

import java.io.File

object CleanWriteServer extends App {
  def delete(file: File): Unit = {
    if (file.isDirectory) file.listFiles.foreach(delete)
    file.delete()
  }
  delete(new File("fsdb"))
  delete(new File("data"))
  delete(new File("data-temp"))
  Server.main(Array("init"))
  Server.main(Array("write"))
}
