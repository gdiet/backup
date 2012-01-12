// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import com.weiglewilczek.slf4s.Logging
import net.diet_rich.util.ScalaThreadLocal
import net.diet_rich.util.sql.WrappedSQLResult
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLSyntaxErrorException
import java.sql.SQLIntegrityConstraintViolationException
import net.diet_rich.util.NextOptIterator
import net.diet_rich.util.io.RandomAccessInput
import net.diet_rich.util.sql._

import DataDefinitions._

class SqlDB(database: DBConnection) extends Logging {
  import SqlDB._

  val settings = database.settings
  
  private val connection = database connection
  private val enableConstraints = true // TODO make configurable
  // EVENTUALLY add/remove constraints independently of database creation
  createTablesIfNotExist (connection, enableConstraints, settings)

  private def prepTLStatement(statement: String) : ScalaThreadLocal[PreparedStatement] =
    ScalaThreadLocal(connection prepareStatement statement, statement)

  private val childrenForIdS = 
    prepTLStatement("SELECT id, name FROM TreeEntries WHERE deleted = false AND parent = ?;")
  private val addEntryS =
    prepTLStatement("INSERT INTO TreeEntries (id, parent, name) VALUES ( ? , ? , ? );")
  private val maxEntryIdS =
    prepTLStatement("SELECT MAX ( id ) AS id FROM TreeEntries;")
  private val containsPrintS =
    prepTLStatement("SELECT COUNT(*) FROM DataInfo JOIN FileData ON DataInfo.id = FileData.data;")

  /** Get the children of an entry from database. Does not retrieve
   *  any children marked deleted.
   */
  def children(id: Long) : List[IdAndName] =
    execQuery(childrenForIdS, id) {rs => IdAndName(rs long "id", rs string "name")} toList

  /** Insert a new entry into the database. */
  def make(id: Long, parent: Long, name: String) : Boolean = {
    try {
      execUpdate(addEntryS, id, parent, name) match {
        case 1 => true
        case n => throw new IllegalStateException("Unexpected " + n + " times update for id " + id)
      }
    } catch {
      case e: SQLIntegrityConstraintViolationException =>
        logger info "Could not add entry: " + id + " / " + e
        false
    }
  }

  /** @return The numerically highest file tree id. */
  def maxEntryID = execQuery(maxEntryIdS) {_.long("id")} head

  def contains(print: TimeSizePrint) : Boolean =
    execQuery(containsPrintS){_ long 1}.head > 0
  
}

object SqlDB extends Logging {

  /** only use where performance is not a critical factor. */
  private def executeDirectly(connection : Connection, command: String, constraints: String = "") : Unit = {
    val fullCommand = command replaceAllLiterally ("- constraints -", constraints)
    val strippedCommand = fullCommand split "\\n" map (_ replaceAll("//.*", "")) mkString ("\n")
    connection.createStatement execute strippedCommand
  }

  def createTablesIfNotExist(connection : Connection, constraintsEnabled: Boolean, settings: FSSettings) : Unit = {
    logger info "checking whether SQL tables are already created"
    try {
      val result = connection.createStatement executeQuery "SELECT value FROM RepositoryInfo WHERE key = 'database version';"
      if (!result.next) throw new IllegalStateException("No database version entry in RepositoryInfo table")
      val dbversion = result getString "value"
      if (dbversion != "1.0") throw new IllegalStateException("Database version " + dbversion + "not supported")
    } catch {
      case e: SQLSyntaxErrorException =>
        if(!e.getMessage.contains("object not found")) throw e
        createTables(connection, constraintsEnabled, settings)
    }
  }

  private def createTables(connection : Connection, constraintsEnabled: Boolean, settings: FSSettings) : Unit = {
    logger info "creating SQL tables"
    // HSQLDB: CACHED tables [...] Only part of their data or indexes is held
    // in memory, allowing large tables that would otherwise take up to several
    // hundred megabytes of memory. [...] The default type of table resulting
    // from future CREATE TABLE statements can be specified with the SQL command:
    executeDirectly(connection, "SET DATABASE DEFAULT TABLE TYPE CACHED;")
    
    // NOTES on SQL syntax used: A PRIMARY KEY constraint is equivalent to a
    // UNIQUE constraint on one or more NOT NULL columns. Only one PRIMARY KEY
    // can be defined in each table.
    // Source: http://hsqldb.org/doc/2.0/guide/guide.pdf
    
    // JOIN is the short form for INNER JOIN.
    
    // TODO check for needed indexes
    
    // The tree is represented by nodes that store their parent but not their children.
    // The tree root can not be deleted and has the ID 0.
    executeDirectly(connection, """
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
      if (!constraintsEnabled) "" else """
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
    executeDirectly(connection, "INSERT INTO TreeEntries (id, parent, name) VALUES ( -1, -1, '*' );")
    executeDirectly(connection, "INSERT INTO TreeEntries (id, parent, name) VALUES (  0, -1, ''  );")
    
    executeDirectly(connection, """
      CREATE TABLE DataInfo (
        id     BIGINT PRIMARY KEY,
        size   BIGINT NOT NULL,                         // entry size (uncompressed)
        print  BIGINT NOT NULL,                         // fast file content fingerprint
        hash   VARBINARY(16) NOT NULL,                  // EVENTUALLY make configurable: MD5: 16, SHA-256: 64
        usage  INTEGER DEFAULT 0 NOT NULL,              // usage count
        method INTEGER DEFAULT 0 NOT NULL               // store method (0 = PLAIN, 1 = DEFLATE)
        - constraints -
      );
      """,
      if (!constraintsEnabled) "" else """
      , CHECK (size >= 0)
      , CHECK (usage >= 0)
      , CHECK (method = 0 OR method = 1)
      , UNIQUE (size, print, hash)
      """
    )
    val zeroByteHash = settings.hashProvider.getHashDigester getDigest
    val zeroBytePrint = settings.printCalculator calculate RandomAccessInput.empty
    execUpdateWithArgs(connection, """
      INSERT INTO DataInfo (id, size, print, hash, usage, method) VALUES ( 0, 0, ?, ?, 0, 0 );
    """, zeroBytePrint, zeroByteHash)

    executeDirectly(connection, """
      CREATE TABLE FileData (
        id      BIGINT PRIMARY KEY,
        time    BIGINT NOT NULL,
        data    BIGINT DEFAULT 0 NOT NULL               // 0 for 0-byte TreeEntries
        - constraints -
      );
      """,
      if (!constraintsEnabled) "" else """
      , FOREIGN KEY (id) REFERENCES TreeEntries(id)
      , FOREIGN KEY (data) REFERENCES DataInfo(id)
      """
    )

    executeDirectly(connection, """
      CREATE TABLE ByteStore (
        id    BIGINT NULL,      // reference to DataInfo#id or NULL if free
        index INTEGER NOT NULL, // data part index
        start BIGINT NOT NULL,  // data part start position
        fin   BIGINT NOT NULL   // data part end position + 1
        - constraints -
      );
      """,
      if (!constraintsEnabled) "" else """
      , UNIQUE (start)
      , UNIQUE (fin)
      , FOREIGN KEY (id) REFERENCES DataInfo(id)
      , CHECK (fin > start AND start >= 0)
      """ // EVENTUALLY check that start has matching fin or is 0
    )
        
    // create and fill RepositoryInfo last so errors in table creation
    // are detected before the "database version" value is inserted.
    executeDirectly(connection, """
      CREATE TABLE RepositoryInfo (
        key   VARCHAR(32) PRIMARY KEY,
        value VARCHAR(256) NOT NULL
      );
      """
    )
    // EVENTUALLY check shut down status on loading the database
    // for this, insert ( 'shut down', 'OK' ) on shutdown and
    // remove the key/value pair on startup.
    executeDirectly(connection, "INSERT INTO RepositoryInfo (key, value) VALUES ( 'shut down', 'OK' );")
    executeDirectly(connection, "INSERT INTO RepositoryInfo (key, value) VALUES ( 'database version', '1.0' );")
    executeDirectly(connection, "INSERT INTO RepositoryInfo (key, value) VALUES ( 'constraints enabled', '" + constraintsEnabled + "' );")
  }

}