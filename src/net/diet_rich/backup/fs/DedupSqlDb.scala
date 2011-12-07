package net.diet_rich.backup.fs

import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLIntegrityConstraintViolationException
import net.diet_rich.util.Choice
import net.diet_rich.util.EnhancedIterator
import net.diet_rich.util.ScalaThreadLocal

class DedupSqlDb {
  // configuration
  private val enableConstraints = true // EVENTUALLY make configurable
  
  // set up connection
  Class forName "org.hsqldb.jdbc.JDBCDriver"
  private val connection = DriverManager getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "")
  connection setAutoCommit true

  // create tables
  private def createTables = {
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
  
  // FIXME implement a possibility to skip DB creation
  createTables

  // JDBC helper methods
  private def executeDirectly(command: String, constraints: String = "") : Unit = {
    val fullCommand = command replaceAllLiterally("- constraints -", if (enableConstraints) constraints else "")
    val strippedCommand = fullCommand split "\\n" map (_ replaceAll("//.*", "")) mkString ("\n")
    connection.createStatement execute strippedCommand
  }
  
  private def prepareStatement(statement: String) = ScalaThreadLocal(connection prepareStatement statement)
  
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

  private def executeUpdate(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) : Int = {
    setArguments(preparedStatement, args:_*) executeUpdate
  }

  /** Note: Calling next(Option) invalidates any previous result objects! */
  private def executeQueryIter(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) : EnhancedIterator[WrappedResult] = {
    val resultSet = setArguments(preparedStatement, args:_*) .executeQuery
    new EnhancedIterator[WrappedResult] {
      def hasNext = resultSet next
      val next = new WrappedResult(resultSet)
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
      executeUpdate(addEntryPS, id, parent, name) > 0
    } catch {
      case e: SQLIntegrityConstraintViolationException => false
    }
  }
  
  //// END OF DATABASE ACCESS TO CACHE
    
}
