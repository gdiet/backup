package dedup
package db

import java.sql.{Connection, DriverManager}

object H2:
  Class.forName("org.h2.Driver")

  // For SQL debugging, add to the DB URL "...;TRACE_LEVEL_SYSTEM_OUT=2"
  private def jdbcUrl(directory: java.io.File, readonly: Boolean) =
    if readonly then s"jdbc:h2:$directory/dedupfs;ACCESS_MODE_DATA=r"
    else s"jdbc:h2:$directory/dedupfs;DB_CLOSE_ON_EXIT=FALSE"

  def connection(directory: java.io.File, readonly: Boolean, settings: String = ""): Connection =
    if !readonly then
      val dbTraceFile = java.io.File(directory, "fsdb/dedupfs.trace.db")
      require(!dbTraceFile.exists(), s"Database trace file $connection found. Check for database problems.")
    DriverManager.getConnection(jdbcUrl(directory, readonly) + settings, "sa", "").tap(_.setAutoCommit(true))
end H2
