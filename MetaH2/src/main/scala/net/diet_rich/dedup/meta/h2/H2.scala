package net.diet_rich.dedup.meta.h2

object H2 {
  val driver = "org.h2.Driver"
  def url(directory: java.io.File) = s"jdbc:h2:$directory/dedupfs;DB_CLOSE_ON_EXIT=FALSE"
  val user = "sa"
  val password = ""
  val onShutdown = Some("SHUTDOWN COMPACT")

  Class forName driver
}
