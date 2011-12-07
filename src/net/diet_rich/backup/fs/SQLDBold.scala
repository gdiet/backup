// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.fs

import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import net.diet_rich.util.Choice
import net.diet_rich.util.EnhancedIterator
import net.diet_rich.util.ScalaThreadLocal

class SQLDBold {
  // configuration
  val enableConstraints = true // EVENTUALLY make configurable
  
  // set up connection
  Class forName "org.hsqldb.jdbc.JDBCDriver"
  val connection = DriverManager getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "")
  connection setAutoCommit true

  // create tables
  def createTables = {
    executeDirectly(
      """
      CREATE TABLE Entries (
        id          BIGINT PRIMARY KEY,
        parent      BIGINT NOT NULL,
        name        VARCHAR(256) NOT NULL,
        type        VARCHAR(4) DEFAULT 'DIR' NOT NULL,  // DIR or FILE
        deleted     BOOLEAN DEFAULT FALSE NOT NULL,
        deleteTime  BIGINT DEFAULT 0 NOT NULL           // timestamp if marked deleted, else 0
        - constraints -
      );
      """,
      """
      , FOREIGN KEY (parent) REFERENCES Entries(id)     // reference integrity of parent
      , UNIQUE (parent, name, deleted, deleteTime)      // unique entries only
      , CHECK (parent != id OR id = 0)                  // no self references
      , CHECK (deleted OR deleteTime = 0)               // defined deleted status
      , CHECK ((id = 0) = (parent = 0))                 // root's parent is 0
      , CHECK (id != 0 OR deleted = FALSE)              // can't delete root
      , CHECK (type = 'DIR' OR type = 'FILE')           // no other types yet
      """
    )
    executeDirectly("INSERT INTO Entries (id, parent, name) VALUES ( 0, 0, '' );")
  }
  
  // FIXME implement a possibility to skip DB creation
  createTables

  // JDBC helper methods
  def executeDirectly(command: String, constraints: String = "") : Unit = {
    val fullCommand = command replaceAllLiterally("- constraints -", if (enableConstraints) constraints else "")
    val strippedCommand = fullCommand split "\\n" map (_ replaceAll("//.*", "")) mkString ("\n")
    connection.createStatement execute strippedCommand
  }
  
  def prepareStatement(statement: String) = ScalaThreadLocal(connection prepareStatement statement)
  
  def setArguments(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) = {
    val statement = preparedStatement()
    args.zipWithIndex foreach(_ match {
      case (x : Long, index)    => statement setLong (index+1, x)
      case (x : Int, index)     => statement setInt (index+1, x)
      case (x : String, index)  => statement setString (index+1, x)
      case (x : Boolean, index) => statement setBoolean (index+1, x)
    })
    statement
  }

  def executeUpdate(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) : Int = {
    setArguments(preparedStatement, args:_*) executeUpdate
  }

  /** Note: Calling next(Option) invalidates any previous result objects! */
  def executeQueryIter(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) : EnhancedIterator[WrappedResult] = {
    val resultSet = setArguments(preparedStatement, args:_*) .executeQuery
    new EnhancedIterator[WrappedResult] {
      def hasNext = resultSet next
      val next = new WrappedResult(resultSet)
    }
  }

  class WrappedResult(resultSet: ResultSet) {
    def long(column: Int)           = resultSet getLong column
    def long(column: String)        = resultSet getLong column
    def longOption(column: Int)     = Choice nullIsNone (resultSet getLong column)
    def longOption(column: String)  = Choice nullIsNone (resultSet getLong column)
    def string(column: Int)         = resultSet getString column
    def string(column: String)      = resultSet getString column
  }

  val getEntryForIdPS = 
    prepareStatement("SELECT parent, name, type FROM Entries WHERE deleted = false AND id = ?;")
  val getChildrenForIdPS = 
    prepareStatement("SELECT id, name, type FROM Entries WHERE deleted = false AND parent = ?;")
//  val getEntryForNameAndParentPS = 
//    prepareStatement("SELECT id, parent, name, type FROM Entries WHERE deleted = false AND name = ? AND parent = ?;")
//  val addEntryIdParentNameTypePS = 
//    prepareStatement("INSERT INTO Entries (id, parent, name, type) VALUES ( ? , ? , ? , ? );")

  case class DBEntryData(id: Long, parent: Long, name: String, typ: String)

  /** Get the entry data from database if any. */
  def dbGet(id: Long) : Option[DBEntryData] = {
    executeQueryIter(getEntryForIdPS, id)
    .nextOption
    .map(rs => DBEntryData(id, rs long "parent", rs string "name", rs string "type"))
  }

  /** Get the children of an entry from database. Returns Nil also if entry is not a directory. */
  def dbGetChildren(id: Long) : List[DBEntryData] = {
    Choice.transformIf(
      executeQueryIter(getChildrenForIdPS, id)
      .map(rs => DBEntryData(rs long "id", id, rs string "name", rs string "type"))
      .toList,
      id == 0
    ){ _ filterNot (_.id == 0) }
  }
  
  //// END OF DATABASE ACCESS
  
  /** Get the entry for an ID if any. */
  def get(id: Long) : Option[DBEntry] = {
    synchronized()
    dbGet(id)
    .map { case DBEntryData(id, parent, name, typ) => 
      typ match {
        case "DIR" =>
          DBDir(id, name, parent, dbGetChildren(id) map (_ id))
        case "FILE" => throw new UnsupportedOperationException // FIXME
        case _ => throw new IllegalArgumentException
      }
    }
  }
  
  /** Create a new directory, return it if successful. */
  def makeDir(id: Long, childName: String) : Option[DBEntry] = {
    synchronized()
    dbGet(id)
    .flatMap { case DBEntryData(id, parent, name, typ) => 
      typ match {
        case "DIR" =>
          if (dbGetChildren(id).exists(_.name == childName)) None
          else {
            None
          }
        case _ => None
      }
    }
//    get(id) flatMap (_ match {
//      case dir: DBDir =>
//        if ()
//        None
//      case _ => None
//    })
//    entry =>
//      rs string "type" match {
//        case "DIR" =>
//          if (executeQueryIter(getChildrenForIdPS, id).exists(_.string("name") == childName)) None
//          else None // FIXME insert
//        case _ => None
//      }
//    )
  }

}