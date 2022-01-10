package dedup
package db

import java.sql.{Connection, DriverManager}

object H2:
  Class.forName("org.h2.Driver")

  val dbName = "dedupfs-206" // H2 version suffix
  val dbFileName = s"$dbName.mv.db"

  // For SQL debugging, add to the DB URL "...;TRACE_LEVEL_SYSTEM_OUT=2"
  private def jdbcUrl(dbDir: java.io.File, readonly: Boolean) =
    if readonly then s"jdbc:h2:$dbDir/$dbName;ACCESS_MODE_DATA=r" else s"jdbc:h2:$dbDir/$dbName"

  def connection(dbDir: java.io.File, readonly: Boolean, dbMustExist: Boolean, settings: String = ""): Connection =
    if dbMustExist then
      val dbFile = java.io.File(dbDir, s"$dbFileName")
      require(dbFile.exists(), s"Database file $dbFile does not exist.")
    if !readonly then
      val dbTraceFile = java.io.File(dbDir, s"$dbName.trace.db")
      require(!dbTraceFile.exists(), s"Database trace file $dbTraceFile found. Check for database problems.")
    DriverManager.getConnection(jdbcUrl(dbDir, readonly) + settings, "sa", "").tap(_.setAutoCommit(true))
