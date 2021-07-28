package dedup
package db

import java.sql.{Connection, DriverManager}

object H2:
  Class.forName("org.h2.Driver")

  // For SQL debugging, add to the DB URL "...;TRACE_LEVEL_SYSTEM_OUT=2"
  private def jdbcUrl(dbDir: java.io.File, readonly: Boolean) =
    if readonly then s"jdbc:h2:$dbDir/dedupfs;ACCESS_MODE_DATA=r" else s"jdbc:h2:$dbDir/dedupfs"

  def connection(dbDir: java.io.File, readonly: Boolean, settings: String = ""): Connection =
    if !readonly then
      val dbTraceFile = java.io.File(dbDir, "dedupfs.trace.db")
      require(!dbTraceFile.exists(), s"Database trace file $dbTraceFile found. Check for database problems.")
    DriverManager.getConnection(jdbcUrl(dbDir, readonly) + settings, "sa", "").tap(_.setAutoCommit(true))
