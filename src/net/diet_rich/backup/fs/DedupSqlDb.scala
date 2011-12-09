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
      CREATE TABLE Entries (
        id          BIGINT PRIMARY KEY,
        parent      BIGINT NOT NULL,
        name        VARCHAR(256) NOT NULL,
        deleted     BOOLEAN DEFAULT FALSE NOT NULL,
        deleteTime  BIGINT DEFAULT 0 NOT NULL,          // timestamp if marked deleted, else 0
        UNIQUE (parent, name, deleted, deleteTime)      // unique entries only
        - constraints -
      );
      """,
      """
      , FOREIGN KEY (parent) REFERENCES Entries(id)     // reference integrity of parent
      , CHECK (parent != id OR id = -1)                 // no self references (except for root's parent)
      , CHECK (deleted OR deleteTime = 0)               // defined deleted status
      , CHECK ((id < 1) = (parent = -1))                // root's and root's parent's parent is -1
      , CHECK (id > -2)                                 // regular entries must have a positive id
      , CHECK ((id = 0) = (name = ''))                  // root's and root's parent's name is "", no other empty names
      , CHECK ((id = -1) = (name = '*'))                // root's and root's parent's name is "", no other empty names
      , CHECK (id > 0 OR deleted = FALSE)               // can't delete root nor root's parent
      """
    )
    executeDirectly("INSERT INTO Entries (id, parent, name) VALUES ( -1, -1, '*' );")
    executeDirectly("INSERT INTO Entries (id, parent, name) VALUES (  0, -1, '' );")
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

  // FIXME execUpdate
  private def executeUpdate(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) : Int = {
    logger debug ("SQL: " + preparedStatement + " " + args.mkString("( "," , "," )"))
    setArguments(preparedStatement, args:_*) executeUpdate()
  }

  /** Note: Calling next(Option) invalidates any previous result objects! */
  // FIXME execQuery
  private def executeQueryIter(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) : EnhancedIterator[WrappedResult] = {
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
    prepareStatement("SELECT parent, name FROM Entries WHERE deleted = false AND id = ?;")
  private val getChildrenForIdPS = 
    prepareStatement("SELECT id, name FROM Entries WHERE deleted = false AND parent = ?;")
  private val addEntryPS =
    prepareStatement("INSERT INTO Entries (id, parent, name) VALUES ( ? , ? , ? );")
  private val renamePS =
    prepareStatement("UPDATE Entries SET name = ? WHERE deleted = false AND id = ?;")
  private val markDeletedPS =
    prepareStatement("UPDATE Entries SET deleted = true, deleteTime = ? WHERE deleted = false AND id = ?;")
  private val maxEntryIdPS =
    prepareStatement("SELECT MAX ( id ) AS id FROM ENTRIES;")

  private val getConfigEntryPS =
    prepareStatement("SELECT value FROM RepositoryInfo WHERE key = ?;")
  private val addConfigEntryPS =
    prepareStatement("INSERT INTO RepositoryInfo (key, value) VALUES ( ? , ? );")
  private val deleteConfigEntryPS =
    prepareStatement("DELETE FROM RepositoryInfo WHERE key = ?;")

  case class ParentAndName(parent: Long, name: String)
  case class IdAndName(id: Long, name: String)
  
  //// START OF DATABASE ACCESS TO CACHE
  
  /** Get the entry data from database if any. */
  def dbGetParentAndName(id: Long) : Option[ParentAndName] =
    if (id == 0) Some( ParentAndName(0, "") )
    else {
      executeQueryIter(getEntryForIdPS, id)
      .nextOption
      .map(rs => ParentAndName(rs long "parent", rs string "name"))
    }

  /** Get the children of an entry from database. */
  def dbGetChildrenIdAndName(id: Long) : List[IdAndName] =
    executeQueryIter(getChildrenForIdPS, id)
    .map(rs => IdAndName(rs long "id", rs string "name"))
    .toList
  
  /** Insert a new entry into the database. */
  def dbAddEntry(id: Long, parent: Long, name: String) : Boolean = {
    try {
      executeUpdate(addEntryPS, id, parent, name) match {
        case 1 => true
        case _ => throw new IllegalStateException("id: " + id)
      }
    } catch {
      case e: SQLIntegrityConstraintViolationException =>
        logger info "could not add entry: " + id + " / " + e; 
        false
    }
  }

  /** Rename an entry if it exists. */
  def rename(id: Long, newName: String) : Boolean = {
    try {
      executeUpdate(renamePS, newName, id) match {
        case 0 => logger info "could not rename, not found: " + id; false
        case 1 => true
        case _ => throw new IllegalStateException("id: " + id)
      }
    } catch {
      case e: SQLIntegrityConstraintViolationException => false
    }
  }

  def delete(id: Long) : Boolean = {
    executeUpdate(markDeletedPS, System.currentTimeMillis(), id) match {
      case 0 => logger info "could not delete, not found: " + id; false
      case 1 => true
      case _ => throw new IllegalStateException("id: " + id)
    }
  }
  
  //// END OF DATABASE ACCESS TO CACHE
  
  //// START OF DATABASE ACCESS NOT TO CACHE

  def getConfig(key: String) : Option[String] =
    executeQueryIter(getConfigEntryPS, key) .nextOption map (_ string "value")

  // EVENTUALLY wrap into transaction
  def setConfig(key: String, value: String) : Unit = {
    executeUpdate(deleteConfigEntryPS, key);
    executeUpdate(addConfigEntryPS, key, value);
  }

//  def maxEntryID : Long = executeQueryIter(maxEntryIdPS).nextOption.map(_.long("id")).getOrElse({
//    logger warn "no max id" // FIXME
//    999L
//  })
  def maxEntryID = executeQueryIter(maxEntryIdPS).next.long("id")
  
  //// END OF DATABASE ACCESS NOT TO CACHE
    
}
