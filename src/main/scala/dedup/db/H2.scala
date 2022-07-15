package dedup
package db

import java.sql.{Connection, DriverManager}

object H2:
  Class.forName("org.h2.Driver")

  val dbName = "dedupfs-210" // H2 version suffix
  val dbFileName = s"$dbName.mv.db"

  // For SQL debugging, add to the DB URL "...;TRACE_LEVEL_SYSTEM_OUT=2"
  private def jdbcUrl(dbDir: java.io.File, readonly: Boolean) =
    if readonly then
      s"jdbc:h2:$dbDir/$dbName;ACCESS_MODE_DATA=r"
    else
      s"jdbc:h2:$dbDir/$dbName;DB_CLOSE_ON_EXIT=FALSE"

  def checkForTraceFile(dbDir: java.io.File): Unit =
    val dbTraceFile = java.io.File(dbDir, s"$dbName.trace.db")
    ensure("h2.trace.file", !dbTraceFile.exists, s"Database trace file $dbTraceFile found. Check for database problems.")

  def dbFile(dbDir: java.io.File, fileNameSuffix: String = ""): java.io.File =
    java.io.File(dbDir, dbFileName + fileNameSuffix)

  def connection(dbDir: java.io.File, readonly: Boolean, expectExists: Boolean = true): Connection =
    ensure("h2.connection", dbFile(dbDir).exists == expectExists,
      s"Database file ${dbFile(dbDir)} does ${if expectExists then "not " else ""}exist.")
    if !readonly then checkForTraceFile(dbDir)
    DriverManager.getConnection(jdbcUrl(dbDir, readonly), "sa", "").tap(_.setAutoCommit(true))
