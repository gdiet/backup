package dedup2

import java.sql.{Connection, DriverManager}

object H2 {
  Class forName "org.h2.Driver"

  // ;TRACE_LEVEL_SYSTEM_OUT=2"
  private def jdbcUrl(directory: java.io.File) = s"jdbc:h2:$directory/dedupfs;DB_CLOSE_ON_EXIT=FALSE"

  def rw(directory: java.io.File): Connection =
    DriverManager.getConnection(jdbcUrl(directory), "sa", "").tap(_.setAutoCommit(true))

  def mem(): Connection =
    DriverManager.getConnection("jdbc:h2:mem:", "sa", "").tap(_.setAutoCommit(true))
}