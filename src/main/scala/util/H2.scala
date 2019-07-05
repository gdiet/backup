package util

import java.sql.{Connection, DriverManager}
import scala.util.chaining._

object H2 {
  Class forName "org.h2.Driver"

  private def jdbcUrl(directory: java.io.File) = s"jdbc:h2:$directory/dedupfs;DB_CLOSE_ON_EXIT=FALSE"
  private def jdbcMemoryUrl = s"jdbc:h2:mem:dedupfs"
  private val defaultUser = "sa"
  private val defaultPassword = ""

  val onShutdown = "SHUTDOWN COMPACT"

  def apply(directory: java.io.File): Connection =
    DriverManager.getConnection(jdbcUrl(directory), defaultUser, defaultPassword).tap(_.setAutoCommit(true))
  def memDb(): Connection =
    DriverManager.getConnection(jdbcMemoryUrl, defaultUser, defaultPassword).tap(_.setAutoCommit(true))
}
