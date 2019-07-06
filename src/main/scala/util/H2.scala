package util

import java.sql.{Connection, DriverManager}

import scala.util.chaining._

object H2 {
  Class forName "org.h2.Driver"

  // ;TRACE_LEVEL_SYSTEM_OUT=2"
  private def jdbcUrl(directory: java.io.File) = s"jdbc:h2:$directory/dedupfs;DB_CLOSE_ON_EXIT=FALSE"

  private val dbUser = "sa"
  private val dbPassword = ""

  val onShutdown = "SHUTDOWN COMPACT"

  def rw(directory: java.io.File): Connection =
    DriverManager.getConnection(jdbcUrl(directory), dbUser, dbPassword).tap(_.setAutoCommit(true))

  def ro(directory: java.io.File): Connection =
    DriverManager.getConnection(jdbcUrl(directory) + ";ACCESS_MODE_DATA=r", dbUser, dbPassword).tap(_.setAutoCommit(true))
}
