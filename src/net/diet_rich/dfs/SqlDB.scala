// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import com.weiglewilczek.slf4s.Logging
import net.diet_rich.util.Configuration._
import net.diet_rich.util.sql._
import java.sql._

import DataDefinitions._

class SqlDB(database: DBConnection) extends SqlCommon with SqlForTree with SqlForFiles with Logging {
  import SqlDB._

  protected val connection : Connection = database connection
  protected def repositoryInfo(key: String) =
    fetchOnlyString(connection, "SELECT value FROM RepositoryInfo WHERE key = '%s'" format key)

  // FIXME use this information
  val settingsInDb : StringMap =
    Map(
      "hash algorithm" -> repositoryInfo("hash algorithm"),
      "print length" -> repositoryInfo("print length")
    )
    
  private val addEntry_ =
    prepare("INSERT INTO TreeEntries (id, parent, name) VALUES ( ? , ? , ? );")
  private val maxEntryId_ =
    prepare("SELECT MAX ( id ) AS id FROM TreeEntries;")
  private val maxFileId_ =
    prepare("SELECT MAX ( fileid ) AS fileid FROM TreeEntries;")
  private val maxCreateTime_ =
    prepare("SELECT MAX ( createtime ) AS createtime FROM TreeEntries;")
  private val matchesForPrint_ =
    prepare("SELECT COUNT(*) FROM DataInfo JOIN FileData ON DataInfo.id = FileData.dataid;")
  private val fileId_ =
    prepare("SELECT FileData.id FROM FileData JOIN DataInfo" +
    		" ON  DataInfo.id    = FileData.dataid" +
    		" AND FileData.time  = ?" +
    		" AND DataInfo.size  = ?" +
    		" AND DataInfo.print = ?" +
    		" AND DataInfo.hash  = ? ;")

  def createNewNode(id: Long, parent: Long, name: String) : Boolean =
    // Note: This method MUST check that there is not yet a child with the same name.
    try {
      execUpdate(addEntry_, id, parent, name) match {
        case 1 => true
        case n => throw new IllegalStateException("Unexpected " + n + " times update for id " + id)
      }
    } catch {
      
      case e: SQLIntegrityConstraintViolationException => // HSQLDB
        if (e.getMessage contains "unique constraint or index violation")
          logger.info("Could not add entry - already present: " + parent + "/" + id, e)
        else if (e.getMessage contains "foreign key no parent")
          logger.info("Could not add entry - parent does not exist: " + parent + "/" + id, e)
        else throw e
        false
        
      case e: org.h2.jdbc.JdbcSQLException => // H2
        if (e.getMessage contains "Unique index or primary key violation")
          logger.info("Could not add entry - already present: " + parent + "/" + id, e)
        else if (e.getMessage contains "Referential integrity constraint violation")
          logger.info("Could not add entry - parent does not exist: " + parent + "/" + id, e)
        else throw e
        false
    }

  /** @return The numerically highest file tree id. */
  def maxEntryID = fetchOnlyLong(maxEntryId_)

  /** @return The numerically highest file entry id. */
  def maxFileID = fetchOnlyLong(maxFileId_)

  /** @return The numerically highest entry creation time stamp. */
  def maxCreateTime = fetchOnlyLong(maxCreateTime_)
  
  def contains(print: TimeSizePrint) : Boolean =
    fetchOnlyLong(matchesForPrint_) > 0

  /** @return The matching data entry ID if any. */
  def fileId(print: TimeSizePrintHash) : Option[Long] =
    execQuery(fileId_, print time, print size, print print, print hash)(_ long 1)
    .headOptionOnly

}

object SqlDB extends Logging {

  /** only use where performance is not a critical factor. */
  private def execWithConstraints(connection : Connection, command: String, constraints: String) : Unit = {
    val fullCommand = command replaceAllLiterally ("- constraints -", constraints)
    val strippedCommand = fullCommand split "\\n" map (_ replaceAll("//.*", "")) mkString ("\n")
    connection.createStatement execute strippedCommand
  }

  def hasTablesFor(dbcon : DBConnection) : Boolean = {
    logger info "checking whether SQL tables already exist"
    try {
      val dbversion = fetchOnlyString(dbcon.connection, "SELECT value FROM RepositoryInfo WHERE key = 'database version';")
      if (dbversion != "1.0") throw new IllegalStateException("Database version " + dbversion + " not supported")
      true
    } catch {
      case e: NoSuchElementException =>
        throw new IllegalStateException("No database version entry in RepositoryInfo table")
      case e: SQLSyntaxErrorException =>
        if(!e.getMessage.contains("object not found")) throw e
        false
    }
  }
  
  // EVENTUALLY add/remove constraints independently of database creation
  // e.g. ALTER TABLE DATAINFO ADD CONSTRAINT NoNegativeSize CHECK (size >= 0);
  def createTables(dbcon: DBConnection, settings: DBSettings /* TODO remove when constraints are separate */, fsSettings: FSSettings) : Unit = {
    logger info "creating SQL tables"
    val connection = dbcon.connection
    
    // NOTES on SQL syntax used: A PRIMARY KEY constraint is equivalent to a
    // UNIQUE constraint on one or more NOT NULL columns. Only one PRIMARY KEY
    // can be defined in each table.
    // (Source: http://hsqldb.org/doc/2.0/guide/guide.pdf)
    
    // JOIN is the short form for INNER JOIN.
    
    // TODO check for needed indexes
    // FIXME CREATE INDEX TreeEntries_dataid_idx on TREEENTRIES (dataid);
    
    execWithConstraints(connection, """
      CREATE CACHED TABLE DataInfo (
        id     BIGINT PRIMARY KEY,
        length BIGINT NOT NULL,                         // entry size (uncompressed)
        print  BIGINT NOT NULL,                         // fast file content fingerprint
        hash   VARBINARY(16) NOT NULL,                  // EVENTUALLY make configurable: MD5: 16, SHA-256: 64
        method INTEGER DEFAULT 0 NOT NULL               // store method (0 = PLAIN, 1 = DEFLATE)
        - constraints -
      );
      """,
      if (!settings.enableConstraints) "" else """
      , CHECK (length >= 0)
      , CHECK (method = 0 OR method = 1)
      , UNIQUE (length, print, hash)
      """
    )
    val zeroByteHash = fsSettings.hashProvider.getHashDigester.digest
    val zeroBytePrint = fsSettings.hashProvider.getPrintDigester.digest
    execUpdate(connection, """
      INSERT INTO DataInfo (id, length, print, hash, method) VALUES ( 0, 0, ?, ?, 0 );
    """, zeroBytePrint, zeroByteHash)

    // The tree is represented by nodes that store their parent but not their children.
    // The tree root can not be deleted and has the ID 0.
    execWithConstraints(connection, """
      CREATE CACHED TABLE TreeEntries (
        id          BIGINT PRIMARY KEY,
        parent      BIGINT NOT NULL,
        name        VARCHAR(256) NOT NULL,
        time        BIGINT DEFAULT NULL,                // not NULL iff dataid is not NULL
        dataid      BIGINT DEFAULT NULL,                // 0 for 0-byte TreeEntries
        UNIQUE (parent, name)                           // unique TreeEntries only
        - constraints -
      );
      """,
      if (!settings.enableConstraints) "" else """
      , FOREIGN KEY (parent) REFERENCES TreeEntries(id) // reference integrity of parent
      , FOREIGN KEY (dataid) REFERENCES DataInfo(id)    // reference integrity of data info pointer
      , CHECK (parent != id OR id = -1)                 // no self references (except for root's parent)
      , CHECK ((dataid is NULL) = (time is NULL))       // timestamp iff data is present
      , CHECK ((id < 1) = (parent = -1))                // root's and root's parent's parent is -1
      , CHECK (id > -2)                                 // regular TreeEntries must have a positive id
      , CHECK ((id = 0) = (name = ''))                  // root's name is "", no other empty names
      , CHECK ((id != -1) OR (name = '*'))              // root's parent's name is "*"
      """
    )
    execUpdate(connection, "INSERT INTO TreeEntries (id, parent, name) VALUES ( -1, -1, '*' );")
    execUpdate(connection, "INSERT INTO TreeEntries (id, parent, name) VALUES (  0, -1,  '' );")
    
    execWithConstraints(connection, """
      CREATE CACHED TABLE ByteStore (
        dataid BIGINT NULL,      // reference to DataInfo#id or NULL if free
        index  INTEGER NOT NULL, // data part index
        start  BIGINT NOT NULL,  // data part start position
        fin    BIGINT NOT NULL   // data part end position + 1
        - constraints -
      );
      """,
      if (!settings.enableConstraints) "" else """
      , UNIQUE (start)
      , UNIQUE (fin)
      , FOREIGN KEY (dataid) REFERENCES DataInfo(id)
      , CHECK (fin > start AND start >= 0)
      """ // EVENTUALLY check that start has matching fin or is 0
    )
        
    // create and fill RepositoryInfo last so errors in table creation
    // are detected before the "database version" value is inserted.
    execUpdate(connection, """
      CREATE CACHED TABLE RepositoryInfo (
        key   VARCHAR(32) PRIMARY KEY,
        value VARCHAR(256) NOT NULL
      );
      """
    )
    // EVENTUALLY check shut down status on loading the database
    // for this, insert ( 'shut down', 'OK' ) on shutdown and
    // remove the key/value pair on startup.
    execUpdate(connection, "INSERT INTO RepositoryInfo (key, value) VALUES ( 'shut down', 'OK' );")
    execUpdate(connection, "INSERT INTO RepositoryInfo (key, value) VALUES ( 'database version', '1.0' );")
    execUpdate(connection, "INSERT INTO RepositoryInfo (key, value) VALUES ( 'constraints enabled', ? );", settings.enableConstraints toString)
    execUpdate(connection, "INSERT INTO RepositoryInfo (key, value) VALUES ( 'hash algorithm', ? );", fsSettings.hashProvider.name) // FIXME get print provider by name
    execUpdate(connection, "INSERT INTO RepositoryInfo (key, value) VALUES ( 'print length', ? );", fsSettings.printLength toString)
  }

}