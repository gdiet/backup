package dedup2

import java.io.File

object CleanInit extends App {
  sys.props.update("LOG_BASE", "./")
  //  def delete(dir: File): Unit = Files.walk(dir.toPath).sorted(reverseOrder).forEach(Files.delete _)
  def delete(file: File): Unit = {
    if (file.isDirectory) file.listFiles.foreach(delete)
    file.delete()
  }
  delete(new File("fsdb"))
  delete(new File("data"))
  delete(new File("dedupfs-temp"))
  Main.main(Array("init"))
}
