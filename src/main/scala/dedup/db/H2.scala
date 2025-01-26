package dedup
package db

import dedup.util.ClassLogging

import java.io.File
import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Date

object H2 extends ClassLogging:

  case class DBRef(dbName: String, suffix: String = ""):
    def dbFile(dbDir: File) = File(dbDir, s"$dbName$suffix.mv.db")
    def dbTraceFile(dbDir: File) = File(dbDir, s"$dbName$suffix.trace.db")
    def dbScriptPath(dbDir: File) = File(dbDir, dbName + suffix)
    def dbZipFile(dbDir: File): File =
      val dateString = SimpleDateFormat("yyyy-MM-dd_HH-mm").format(Date())
      File(dbDir, s"${dbName}_$dateString$suffix.zip")
    def dbZipNamePattern: String =
      s"${dbName}_\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}$suffix.zip"
    def backup: DBRef = DBRef(dbName, s"${suffix}_backup")
    def beforeUpgrade: DBRef = DBRef(dbName, s"${suffix}_before_upgrade")
    def beforeReclaim: DBRef = DBRef(dbName, s"${suffix}_before_reclaim")
    def beforeBlacklisting: DBRef = DBRef(dbName, s"${suffix}_before_blacklisting")

  Class.forName("org.h2.Driver")
  private def tcpPortProp = sys.props.get(s"H2.TcpPort")

  val dbRef: DBRef = DBRef("dedupfs-232") // H2 version 232 suffix since 6.0, can stay for as long as the storage format is binary compatible.
  val previousDbRef: DBRef = DBRef("dedupfs-210") // Used with version 5.x.

  // For SQL debugging, add to the DB URL "...;TRACE_LEVEL_SYSTEM_OUT=2"
  // To connect to a local H2 TCP server e.g. for demonstration or debugging purposes,
  // add -DH2.TcpPort=<TCP port> to the Java arguments of the application.
  // The TCP server can be run from command line or programmatically like this:
  // java -cp "h2-2.1.214.jar" org.h2.tools.Server -tcp -tcpPort 9876
  // org.h2.tools.Server.main("-tcp", "-tcpPort", "9876")
  def jdbcUrl(dbDir: File, readOnly: Boolean, dbRef: DBRef = dbRef): String =
    tcpPortProp match
      case None =>
        val baseUrl = s"jdbc:h2:${dbRef.dbScriptPath(dbDir)}"
        if readOnly then
          s"$baseUrl;ACCESS_MODE_DATA=r"
        else
          s"$baseUrl;DB_CLOSE_ON_EXIT=FALSE;MAX_COMPACT_TIME=2000"
      case Some(tcpPort) =>
        val baseUrl = s"jdbc:h2:tcp://localhost:$tcpPort/${dbRef.dbScriptPath(dbDir)}"
        dedup.main.warn(s"Running with a local H2 database server. Connect using this URL:")
        dedup.main.warn(baseUrl)
        dedup.main.warn(s"User    : sa")
        dedup.main.warn(s"Password: [empty]")
        if readOnly then
          s"$baseUrl;ACCESS_MODE_DATA=r"
        else
          s"$baseUrl;DB_CLOSE_ON_EXIT=FALSE;MAX_COMPACT_TIME=2000"

  def checkForTraceFile(dbDir: File): Unit =
    ensure("h2.trace.file", !dbRef.dbTraceFile(dbDir).exists,
      s"Database trace file ${dbRef.dbTraceFile(dbDir)} found. Check for database problems.")

  def connection(dbDir: File, readOnly: Boolean, expectExists: Boolean = true): Connection =
    ensure("h2.previousDb", !previousDbRef.dbFile(dbDir).exists(),
      s"A database file from an earlier version of this software exists in $dbDir.")
    ensure("h2.connection", dbRef.dbFile(dbDir).exists == expectExists,
      s"Database file ${dbRef.dbFile(dbDir)} does ${if expectExists then "not " else ""}exist.")
    if !readOnly then checkForTraceFile(dbDir)
    DriverManager.getConnection(jdbcUrl(dbDir, readOnly), "sa", "").tap(_.setAutoCommit(true))

  def shutdownCompact(connection: Connection): Unit =
    if tcpPortProp.isDefined then connection.close() // Another application might still be connected to the database.
    else { log.info("Compacting DedupFS database..."); connection.createStatement().execute("SHUTDOWN COMPACT") }
