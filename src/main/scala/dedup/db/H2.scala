package dedup
package db

import java.sql.{Connection, DriverManager}

object H2:
  Class.forName("org.h2.Driver")

  val dbName = "dedupfs-210" // H2 version suffix, can stay 210 for as long as the storage format is binary compatible.
  val dbFileName = s"$dbName.mv.db"
  def dbFile(dbDir: java.io.File): java.io.File = java.io.File(dbDir, dbFileName)

  val backupName = s"${dbName}_backup"
  val backupFileName = s"$backupName.mv.db"
  def backupFile(dbDir: java.io.File): java.io.File = java.io.File(dbDir, backupFileName)

  // For SQL debugging, add to the DB URL "...;TRACE_LEVEL_SYSTEM_OUT=2"
  private def jdbcUrl(dbDir: java.io.File, readOnly: Boolean) =
    if readOnly then
      s"jdbc:h2:$dbDir/$dbName;ACCESS_MODE_DATA=r"
    else
      s"jdbc:h2:$dbDir/$dbName;DB_CLOSE_ON_EXIT=FALSE;MAX_COMPACT_TIME=2000"

  def checkForTraceFile(dbDir: java.io.File): Unit =
    val dbTraceFile = java.io.File(dbDir, s"$dbName.trace.db")
    ensure("h2.trace.file", !dbTraceFile.exists, s"Database trace file $dbTraceFile found. Check for database problems.")

  def connection(dbDir: java.io.File, readOnly: Boolean, expectExists: Boolean = true): Connection =
    ensure("h2.connection", dbFile(dbDir).exists == expectExists,
      s"Database file ${dbFile(dbDir)} does ${if expectExists then "not " else ""}exist.")
    if !readOnly then checkForTraceFile(dbDir)
    DriverManager.getConnection(jdbcUrl(dbDir, readOnly), "sa", "").tap(_.setAutoCommit(true))
