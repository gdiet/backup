package net.diet_rich.backup.fs

import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLIntegrityConstraintViolationException
import net.diet_rich.util.Choice
import net.diet_rich.util.EnhancedIterator
import net.diet_rich.util.ScalaThreadLocal
import com.weiglewilczek.slf4s.Logging
import java.sql.SQLSyntaxErrorException

class DedupSqlDb extends Logging {
  // configuration
  private val enableConstraints = true // EVENTUALLY make configurable
  
  // FIXME database shutdown:
  // A special form of closing the database is via the SHUTDOWN COMPACT command. 
  // This command rewrites the .data file that contains the information stored 
  // in CACHED tables and compacts it to its minimum size. This command should 
  // be issued periodically, especially when lots of inserts, updates or deletes 
  // have been performed on the cached tables. Changes to the structure of the 
  // database, such as dropping or modifying populated CACHED tables or indexes 
  // also create large amounts of unused file space that can be reclaimed using 
  // this command.
  
  Class forName "org.hsqldb.jdbc.JDBCDriver"
  // FIXME make database location configurable
  private val connection = DriverManager getConnection("jdbc:hsqldb:file:temp/testdb", "SA", "")

  connection setAutoCommit true

  // create tables
  private def createTables : Unit = {
    logger info "creating SQL tables"
    // HSQLDB: CACHED tables [...] Only part of their data or indexes is held
    // in memory, allowing large tables that would otherwise take up to several
    // hundred megabytes of memory. [...] The default type of table resulting
    // from future CREATE TABLE statements can be specified with the SQL command:
    executeDirectly("SET DATABASE DEFAULT TABLE TYPE CACHED;")
    executeDirectly("""
      CREATE TABLE RepositoryInfo (
        key   VARCHAR(32) PRIMARY KEY,
        value VARCHAR(256) NOT NULL
      );
      """
    )
    // EVENTUALLY check shut down status on loading the database
    // for this, insert ( 'shut down', 'OK' ) on shutdown and
    // remove the key/value pair on startup.
    executeDirectly("INSERT INTO RepositoryInfo (key, value) VALUES ( 'shut down', 'OK' );")
    executeDirectly("INSERT INTO RepositoryInfo (key, value) VALUES ( 'database version', '1.0' );")
    executeDirectly("INSERT INTO RepositoryInfo (key, value) VALUES ( 'constraints enabled', '" + enableConstraints + "' );")
    executeDirectly(
      """
      CREATE TABLE TreeEntries (
        id          BIGINT PRIMARY KEY,
        parent      BIGINT NOT NULL,
        name        VARCHAR(256) NOT NULL,
        deleted     BOOLEAN DEFAULT FALSE NOT NULL,
        deleteTime  BIGINT DEFAULT 0 NOT NULL,          // timestamp if marked deleted, else 0
        UNIQUE (parent, name, deleted, deleteTime)      // unique TreeEntries only
        - constraints -
      );
      """,
      """
      , FOREIGN KEY (parent) REFERENCES TreeEntries(id) // reference integrity of parent
      , CHECK (parent != id OR id = -1)                 // no self references (except for root's parent)
      , CHECK (deleted OR deleteTime = 0)               // defined deleted status
      , CHECK ((id < 1) = (parent = -1))                // root's and root's parent's parent is -1
      , CHECK (id > -2)                                 // regular TreeEntries must have a positive id
      , CHECK ((id = 0) = (name = ''))                  // root's name is "", no other empty names
      , CHECK ((id != -1) OR (name = '*'))              // root's parent's name is "*"
      , CHECK (id > 0 OR deleted = FALSE)               // can't delete root nor root's parent
      """
    )
    executeDirectly("INSERT INTO TreeEntries (id, parent, name) VALUES ( -1, -1, '*' );")
    executeDirectly("INSERT INTO TreeEntries (id, parent, name) VALUES (  0, -1, '' );")
    
    executeDirectly("""
      CREATE TABLE FileData (
        id      BIGINT UNIQUE NOT NULL,
        time    BIGINT NOT NULL,
        data    BIGINT DEFAULT 0 NOT NULL               // 0 for 0-byte TreeEntries
        - constraints -
      );
      """,
      """
      , FOREIGN KEY (id) REFERENCES TreeEntries(id)
      // , FOREIGN KEY (data) REFERENCES StoredData(id)  FIXME enable with StoredData
      """
    )
  }

  def createTablesIfNotExist : Unit = {
    logger info "checking whether SQL tables are already created"
    try {
      val result = connection.createStatement executeQuery "SELECT value FROM RepositoryInfo WHERE key = 'database version';"
      if (!result.next) throw new IllegalStateException("No database version entry in RepositoryInfo table")
      val dbversion = result getString "value"
      if (dbversion != "1.0") throw new IllegalStateException("Database version " + dbversion + "not supported")
    } catch {
      case e: SQLSyntaxErrorException =>
        if(!e.getMessage.contains("object not found")) throw e
        createTables
    }
  }
  
  createTablesIfNotExist

  // JDBC helper methods
  private def executeDirectly(command: String, constraints: String = "") : Unit = {
    val fullCommand = command replaceAllLiterally("- constraints -", if (enableConstraints) constraints else "")
    val strippedCommand = fullCommand split "\\n" map (_ replaceAll("//.*", "")) mkString ("\n")
    connection.createStatement execute strippedCommand
  }
  
  private def prepareStatement(statement: String) = ScalaThreadLocal(connection prepareStatement statement, statement)
  
  private def setArguments(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) = {
    val statement = preparedStatement()
    args.zipWithIndex foreach(_ match {
      case (x : Long, index)    => statement setLong (index+1, x)
      case (x : Int, index)     => statement setInt (index+1, x)
      case (x : String, index)  => statement setString (index+1, x)
      case (x : Boolean, index) => statement setBoolean (index+1, x)
    })
    statement
  }

  private def execUpdate(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) : Int = {
    logger debug ("SQL: " + preparedStatement + " " + args.mkString("( "," , "," )"))
    setArguments(preparedStatement, args:_*) executeUpdate()
  }

  /** Note: Calling next(Option) invalidates any previous result objects! */
  private def execQuery(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) : EnhancedIterator[WrappedResult] = {
    logger debug ("SQL: " + preparedStatement + " " + args.mkString("( "," , "," )"))
    val resultSet = setArguments(preparedStatement, args:_*) .executeQuery
    val wrappedResult = new WrappedResult(resultSet)
    new EnhancedIterator[WrappedResult] {
      var hasNextIsChecked = false
      var hasNextResult = false
      override def hasNext : Boolean = {
        if (!hasNextIsChecked) {
          hasNextResult = resultSet next()
          hasNextIsChecked = true
        }
        hasNextResult
      }
      override def next : WrappedResult = {
        hasNext
        hasNextIsChecked = false
        wrappedResult
      }
    }
  }

  private class WrappedResult(resultSet: ResultSet) {
    def long(column: Int)           = resultSet getLong column
    def long(column: String)        = resultSet getLong column
    def longOption(column: Int)     = Choice nullIsNone (resultSet getLong column)
    def longOption(column: String)  = Choice nullIsNone (resultSet getLong column)
    def string(column: Int)         = resultSet getString column
    def string(column: String)      = resultSet getString column
  }

  private val getEntryForIdPS = 
    prepareStatement("SELECT parent, name FROM TreeEntries WHERE deleted = false AND id = ?;")
  private val getChildrenForIdPS = 
    prepareStatement("SELECT id, name FROM TreeEntries WHERE deleted = false AND parent = ?;")
  private val addEntryPS =
    prepareStatement("INSERT INTO TreeEntries (id, parent, name) VALUES ( ? , ? , ? );")
  private val renamePS =
    prepareStatement("UPDATE TreeEntries SET name = ? WHERE deleted = false AND id = ?;")
  private val markDeletedPS =
    prepareStatement("UPDATE TreeEntries SET deleted = true, deleteTime = ? WHERE deleted = false AND id = ?;")
  private val maxEntryIdPS =
    prepareStatement("SELECT MAX ( id ) AS id FROM TreeEntries;")

  private val getConfigEntryPS =
    prepareStatement("SELECT value FROM RepositoryInfo WHERE key = ?;")
  private val addConfigEntryPS =
    prepareStatement("INSERT INTO RepositoryInfo (key, value) VALUES ( ? , ? );")
  private val deleteConfigEntryPS =
    prepareStatement("DELETE FROM RepositoryInfo WHERE key = ?;")

  private val addFileDataPS =
    prepareStatement("INSERT INTO FileData (id, time, data) VALUES ( ? , ? , ? );")
  private val getFileDataPS =
    prepareStatement("SELECT time, data FROM FileData WHERE id = ?;")
  private val deleteFileDataPS =
    prepareStatement("DELETE FROM FileData WHERE id = ?;")

  case class ParentAndName(parent: Long, name: String)
  case class IdAndName(id: Long, name: String)
  case class TimeAndData(time: Long, data: Long)
  
  //// START OF DATABASE ACCESS TO CACHE

  // Note: currently, it seems like HSQLDB could handle >1000 file operations
  // per second. This would be OK. If it drops below that, caching could
  // greatly increase the speed of file operations.

  //// TREE ENTRY METHODS
  
  /** Get the entry data from database if any.
   *  Does not get entry data for TreeEntries marked deleted.
   */
  def dbGetParentAndName(id: Long) : Option[ParentAndName] =
    if (id == 0) Some( ParentAndName(0, "") )
    else {
      execQuery(getEntryForIdPS, id)
      .nextOption
      .map(rs => ParentAndName(rs long "parent", rs string "name"))
    }

  /** Get the children of an entry from database.
   *  Gets the children of deleted TreeEntries, but does not get
   *  children marked deleted.
   */
  def dbGetChildrenIdAndName(id: Long) : List[IdAndName] =
    execQuery(getChildrenForIdPS, id)
    .map(rs => IdAndName(rs long "id", rs string "name"))
    .toList
  
  /** Insert a new entry into the database. */
  def dbAddEntry(id: Long, parent: Long, name: String) : Boolean = {
    try {
      execUpdate(addEntryPS, id, parent, name) match {
        case 1 => true
        case _ => throw new IllegalStateException("id: " + id)
      }
    } catch {
      case e: SQLIntegrityConstraintViolationException =>
        logger info "could not add entry: " + id + " / " + e; 
        false
    }
  }

  /** Rename an entry if it exists.
   *  Does not rename TreeEntries marked deleted.
   */
  def rename(id: Long, newName: String) : Boolean = {
    try {
      execUpdate(renamePS, newName, id) match {
        case 0 => logger info "could not rename, not found: " + id; false
        case 1 => true
        case _ => throw new IllegalStateException("id: " + id)
      }
    } catch {
      case e: SQLIntegrityConstraintViolationException => false
    }
  }

  /** Mark an entry deleted if it exists.
   *  Does not mark TreeEntries that have already been marked deleted.
   */
  def delete(id: Long) : Boolean = {
    execUpdate(markDeletedPS, System.currentTimeMillis(), id) match {
      case 0 => logger info "could not delete, not found: " + id; false
      case 1 => true
      case _ => throw new IllegalStateException("id: " + id)
    }
  }
  
  //// FILE DATA METHODS
  
  // EVENTUALLY wrap into transaction instead of checking for the exception
  def setFileData(id: Long, time: Long, data: Long) : Boolean = {
    execUpdate(deleteFileDataPS, id)
    try { execUpdate(addFileDataPS, id, time, data); true }
    catch { case e: SQLIntegrityConstraintViolationException => false }
  }

  def getFileData(id: Long) : Option[TimeAndData] =
    execQuery(getFileDataPS, id)
    .nextOption
    .map(rs => TimeAndData(rs long "time", rs long "data"))
  
  def clearFileData(id: Long) : Unit = execUpdate(deleteFileDataPS, id)
  
  //// END OF DATABASE ACCESS TO CACHE
  
  //// START OF DATABASE ACCESS NOT TO CACHE

  def getConfig(key: String) : Option[String] =
    execQuery(getConfigEntryPS, key) .nextOption map (_ string "value")

  // EVENTUALLY wrap into transaction instead of checking for the exception
  def setConfig(key: String, value: String) : Unit = {
    execUpdate(deleteConfigEntryPS, key)
    try execUpdate(addConfigEntryPS, key, value)
    catch { case e: SQLIntegrityConstraintViolationException => /**/ }
  }

  def maxEntryID = execQuery(maxEntryIdPS).next.long("id")
  
  //// END OF DATABASE ACCESS NOT TO CACHE
    
}
