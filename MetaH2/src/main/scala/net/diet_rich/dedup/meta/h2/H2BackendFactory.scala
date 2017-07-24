package net.diet_rich.dedup.meta.h2

import java.io.File

import net.diet_rich.common.io.using
import net.diet_rich.common.sql.ConnectionFactory
import net.diet_rich.dedup.meta.MetaBackendFactory

object H2 {
  val driver = "org.h2.Driver"
  def url(directory: File) = s"jdbc:h2:$directory/dedupfs;DB_CLOSE_ON_EXIT=FALSE"
  val user = "sa"
  val password = ""
  val onShutdown = Some("SHUTDOWN COMPACT")
}

class H2BackendFactory extends MetaBackendFactory {
  override def initialize(directory: File, name: String, hashAlgorithm: String): Unit =
    using(ConnectionFactory(H2.driver, H2.url(directory), H2.user, H2.password, H2.onShutdown)) {
      ???
    }
}
