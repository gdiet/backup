package dedup
package db

import dedup.util.ClassLogging

import java.sql.{Connection, DriverManager}

// TODO the handling of db file names, paths etc. feels brittle and confusing => clean up.
object H2 extends ClassLogging:
  Class.forName("org.h2.Driver")
  private def tcpPortProp = sys.props.get(s"H2.TcpPort")
  
  val dbName = "dedupfs-232" // H2 version 232 suffix since 6.0, can stay for as long as the storage format is binary compatible.
  def dbFile(dbDir: java.io.File): java.io.File = java.io.File(dbDir, s"$dbName.mv.db")
  val previousDbName = "dedupfs-210" // Used with version 5.x.
  def previousDbFile(dbDir: java.io.File): java.io.File = java.io.File(dbDir, s"$previousDbName.mv.db")

  private val backupName = s"${dbName}_backup"
  def backupFile(dbDir: java.io.File): java.io.File = java.io.File(dbDir, s"$backupName.mv.db")

  // For SQL debugging, add to the DB URL "...;TRACE_LEVEL_SYSTEM_OUT=2"
  // To connect to a local H2 TCP server e.g. for demonstration or debugging purposes,
  // add -DH2.TcpPort=<TCP port> to the Java arguments of the application.
  // The TCP server can be run from command line or programmatically like this:
  // java -cp "h2-2.1.214.jar" org.h2.tools.Server -tcp -tcpPort 9876
  // org.h2.tools.Server.main("-tcp", "-tcpPort", "9876")
  private def jdbcUrl(dbDir: java.io.File, readOnly: Boolean) =
    tcpPortProp match
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
        if readOnly then
          s"$baseUrl;ACCESS_MODE_DATA=r"
        else
          s"$baseUrl;DB_CLOSE_ON_EXIT=FALSE;MAX_COMPACT_TIME=2000"

  def checkForTraceFile(dbDir: java.io.File): Unit =
    val dbTraceFile = java.io.File(dbDir, s"$dbName.trace.db")
    ensure("h2.trace.file", !dbTraceFile.exists, s"Database trace file $dbTraceFile found. Check for database problems.")

  def connection(dbDir: java.io.File, readOnly: Boolean, expectExists: Boolean = true): Connection =
    ensure("h2.previousDb", !previousDbFile(dbDir).exists(),
      s"A database file from an earlier version of this software exists in $dbDir.")
    ensure("h2.connection", dbFile(dbDir).exists == expectExists,
      s"Database file ${dbFile(dbDir)} does ${if expectExists then "not " else ""}exist.")
    if !readOnly then checkForTraceFile(dbDir)
    DriverManager.getConnection(jdbcUrl(dbDir, readOnly), "sa", "").tap(_.setAutoCommit(true))

  def shutdownCompact(connection: Connection): Unit =
    if tcpPortProp.isDefined then connection.close() // Another application might still be connected to the database.
    else { log.info("Compacting DedupFS database..."); connection.createStatement().execute("SHUTDOWN COMPACT") }
