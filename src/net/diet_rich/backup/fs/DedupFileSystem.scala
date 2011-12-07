// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.fs
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import net.diet_rich.util.Choice
import net.diet_rich.util.EnhancedIterator
import net.diet_rich.util.ScalaThreadLocal
import java.sql.SQLIntegrityConstraintViolationException

//    path = parent.map(p => if (p.isRoot) p.path else p.path + "/").getOrElse("") + name

class DedupFileSystem(db: DedupSqlDb) {
  def path(path: String) : DPath = DPath(this, path)
  def file(path: String) : Option[DFile] = {
    require((path equals "") || (path matches "/.*[^/]")) // root or starting but not ending with a slash
    if (path == "")
      Some( DFile(this, 0) )
    else
      path.split("/").tail
      .foldLeft(Option(0L))((parent, name) => parent flatMap ( child(_, name) ))
      .map(file _)
  }
  
  def path(id: Long) : Option[DPath] = throw new UnsupportedOperationException
  def file(id: Long) : DFile = DFile(this, id)
  
  def children(id: Long) : List[Long] = db getChildren id
  def exists(id: Long) : Boolean = name(id) isDefined
  def name(id: Long) : Option[String] = db getName id
  def child(id: Long, childName: String) : Option[Long] = {
    require(!childName.contains("/"))
    db getChild (id, childName)
  }
  def mkChild(id: Long, childName: String) : Option[Long] = {
    require(!childName.contains("/"))
    db mkChild (id, childName)
  }
  
  override def toString: String = "fs" // EVENTUALLY write a sensible name
}

/** A file system path identified by the path string. There may not be a file system entry
 *  for the path, and any entry may change all its properties (id, name, parent, children, 
 *  ...) during the path object's lifetime.
 */
case class DPath (val fs: DedupFileSystem, val path: String) {
  require((path equals "") || (path matches "/.*[^/]")) // root or starting but not ending with a slash
  
  def file : Option[DFile] = fs file path
  /** For root, returns root. */
  def parent : DPath = if (isRoot) this else copy( path = path substring (0, path lastIndexOf "/") )
  def isRoot : Boolean = path == ""
  def child(childName: String) : DPath = {
    require(childName matches "/.*[^/]") // starting but not ending with a slash
    DPath(fs, path + "/" + childName)
  }
}

/** A file system entry identified by its ID. The entry may not exist, and it may change
 *  all its properties (name, parent, children, ...) except for its ID during its lifetime.
 */
case class DFile (val fs: DedupFileSystem, val id: Long) {
  def path : Option[DPath] = fs path id
  def children : List[DFile] = fs children id map (copy (fs, _))
  def exists : Boolean = fs exists id
  def name : Option[String] = fs name id
  /** Creating the child will fail if a child with the same name already exists. */
  def mkChild(childName: String) : Option[DFile] = fs mkChild (id, childName) map (copy (fs, _))
}




class DedupSqlDb {
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
    prepareStatement("SELECT parent, name FROM Entries WHERE deleted = false AND id = ?;")
  val getChildrenForIdPS = 
    prepareStatement("SELECT id, name FROM Entries WHERE deleted = false AND parent = ?;")
  val addEntryPS =
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
  
  // FIXME initialize with largest value in database + 1
  val nextEntry = new java.util.concurrent.atomic.AtomicLong(1)
  
  def getName(id: Long) : Option[String] = dbGetParentAndName(id) map (_ name)
  def getChildren(id: Long) : List[Long] = dbGetChildrenIdAndName(id) map (_ id)
  def getChild(id: Long, childName: String) : Option[Long] =
    dbGetChildrenIdAndName(id) find (_.name == childName) map (_.id)
  def mkChild(id: Long, childName: String) : Option[Long] = {
    val childId = nextEntry.getAndIncrement()
    if (dbAddEntry(childId, id, childName)) Some(childId) else None
  }
  
}

