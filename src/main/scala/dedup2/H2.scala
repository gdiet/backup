package dedup2

import java.sql.{Connection, DriverManager}

object H2 {
  Class forName "org.h2.Driver"

  // ;TRACE_LEVEL_SYSTEM_OUT=2"
  private def jdbcUrl(directory: java.io.File, readonly: Boolean) =
    if (readonly) s"jdbc:h2:$directory/dedupfs;DB_CLOSE_ON_EXIT=FALSE;ACCESS_MODE_DATA=r"
    else s"jdbc:h2:$directory/dedupfs;DB_CLOSE_ON_EXIT=FALSE"

  def file(directory: java.io.File, readonly: Boolean): Connection =
    DriverManager.getConnection(jdbcUrl(directory, readonly), "sa", "").tap(_.setAutoCommit(true))

  def mem(): Connection =
    DriverManager.getConnection("jdbc:h2:mem:", "sa", "").tap(_.setAutoCommit(true))
}
