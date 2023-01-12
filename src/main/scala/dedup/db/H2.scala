package dedup
package db

import java.sql.{Connection, DriverManager}

object H2:
  Class.forName("org.h2.Driver")

  val dbName = "dedupfs-210" // H2 version suffix, can stay 210 for as long as the storage format is binary compatible.
  def dbFile(dbDir: java.io.File): java.io.File = java.io.File(dbDir, s"$dbName.mv.db")

  val backupName = s"${dbName}_backup"
  def backupFile(dbDir: java.io.File): java.io.File = java.io.File(dbDir, s"$backupName.mv.db")

  // For SQL debugging, add to the DB URL "...;TRACE_LEVEL_SYSTEM_OUT=2"
  // To run a H2 server and connect to it, e.g. for demonstration or debugging purposes,
  // add -DH2.TcpPort=<TCP port> to the Java arguments of the application.
  private def jdbcUrl(dbDir: java.io.File, readOnly: Boolean) =
    sys.props.get(s"H2.TcpPort") match
      case None =>
        val baseUrl = s"jdbc:h2:$dbDir/$dbName"
        if readOnly then
          s"$baseUrl;ACCESS_MODE_DATA=r"
        else
          s"$baseUrl;DB_CLOSE_ON_EXIT=FALSE;MAX_COMPACT_TIME=2000"
      case Some(tcpPort) =>
        val baseUrl = s"jdbc:h2:tcp://localhost:$tcpPort/$dbDir/$dbName"
        dedup.main.warn(s"Running with a local H2 database server. Connect using this URL:")
        dedup.main.warn(baseUrl)
        dedup.main.warn(s"User    : sa")
        dedup.main.warn(s"Password: [empty]")
        org.h2.tools.Server.main("-tcp", "-tcpPort", tcpPort)
        if readOnly then
          s"$baseUrl;ACCESS_MODE_DATA=r"
        else
          s"$baseUrl;DB_CLOSE_ON_EXIT=FALSE;MAX_COMPACT_TIME=2000"

  def checkForTraceFile(dbDir: java.io.File): Unit =
    val dbTraceFile = java.io.File(dbDir, s"$dbName.trace.db")
    ensure("h2.trace.file", !dbTraceFile.exists, s"Database trace file $dbTraceFile found. Check for database problems.")

  def connection(dbDir: java.io.File, readOnly: Boolean, expectExists: Boolean = true): Connection =
    ensure("h2.connection", dbFile(dbDir).exists == expectExists,
      s"Database file ${dbFile(dbDir)} does ${if expectExists then "not " else ""}exist.")
    if !readOnly then checkForTraceFile(dbDir)
    DriverManager.getConnection(jdbcUrl(dbDir, readOnly), "sa", "").tap(_.setAutoCommit(true))
