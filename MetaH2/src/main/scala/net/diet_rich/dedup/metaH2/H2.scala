package net.diet_rich.dedup.metaH2

object H2 {
  val driver = "org.h2.Driver"
  def jdbcUrl(directory: java.io.File) = s"jdbc:h2:$directory/dedupfs;DB_CLOSE_ON_EXIT=FALSE"
  def jdbcMemoryUrl() = s"jdbc:h2:mem:dedupfs"
  val user = "sa"
  val password = ""
  val onShutdown = Some("SHUTDOWN COMPACT")

  Class forName driver
}
