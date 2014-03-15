package bugreport.h2

object PopulateRepo extends App {
  val target = "target/playRepo"

  val dir = new java.io.File(target)
  dir.mkdirs()
  def del(file: java.io.File): Unit =
    if (file isFile) file delete else {
      file.listFiles foreach del
      file delete
    }
  dir.listFiles foreach del
  
  net.diet_rich.dedup.repository.Create.main(Array(
    "-r", target, "-g", "n"
  )) 
    
  net.diet_rich.dedup.backup.Backup.main(Array(
    "-r", target, "-g", "n", "-s", "src", "-t", "/src", "-i", "n"
  ))
}
