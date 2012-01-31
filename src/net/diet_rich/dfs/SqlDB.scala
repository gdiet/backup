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
import collection.immutable.WrappedString

import DataDefinitions._

class SqlDB(database: DBConnection) extends SqlCommon with SqlForTree with SqlForFiles with Logging {
  import SqlDB._

  protected val connection = database connection

  val settings: FSSettings = {
    val hashAlgorithm =
      execQuery(connection, "SELECT value FROM RepositoryInfo WHERE key = 'hash algorithm'"){_.string("value")}.head
    val printAlgorithm =
      execQuery(connection, "SELECT value FROM RepositoryInfo WHERE key = 'print algorithm'"){_.string("value")}.head
    val printLengthString: WrappedString =
      execQuery(connection, "SELECT value FROM RepositoryInfo WHERE key = 'print length'"){_.string("value")}.head
    FSSettings(hashAlgorithm, printAlgorithm, printLengthString.toInt)
  }

  private val addEntryS =
    prepare("INSERT INTO TreeEntries (id, parent, name) VALUES ( ? , ? , ? );")
  private val maxEntryIdS =
    prepare("SELECT MAX ( id ) AS id FROM TreeEntries;")
  private val maxFileIdS =
    prepare("SELECT MAX ( fileid ) AS fileid FROM TreeEntries;")
  private val maxCreateTimeS =
    prepare("SELECT MAX ( createtime ) AS createtime FROM TreeEntries;")
  private val matchesForPrintS =
    prepare("SELECT COUNT(*) FROM DataInfo JOIN FileData ON DataInfo.id = FileData.dataid;")
  private val fileIdS =
    prepare("SELECT FileData.id FROM FileData JOIN DataInfo" +
    		" ON  DataInfo.id    = FileData.dataid" +
    		" AND FileData.time  = ?" +
    		" AND DataInfo.size  = ?" +
    		" AND DataInfo.print = ?" +
    		" AND DataInfo.hash  = ? ;")

  def make(id: Long, parent: Long, name: String) : Boolean =
    // Note: This method MUST check that there is not yet a child with the same name.
    try {
      println(id + " " + parent + " " + name)
      execUpdate(addEntryS, id, parent, name) match {
        case 1 => true
        case n => throw new IllegalStateException("Unexpected " + n + " times update for id " + id)
      }
    } catch {
      case e: SQLIntegrityConstraintViolationException => // HSQLDB
        if (e.getMessage contains "unique constraint or index violation") {
          println(e.getMessage)
          logger.info("Could not add entry - already present: " + parent + "/" + id, e)
          false
        } else throw e
      case e: org.h2.jdbc.JdbcSQLException => // H2
        if (e.getMessage contains "Unique index or primary key violation") {
          logger.info("Could not add entry - already present: " + parent + "/" + id, e)
          false
        } else throw e
    }

  /** @return The numerically highest file tree id. */
  def maxEntryID = execQuery(maxEntryIdS) {_.long("id")} head

  /** @return The numerically highest file entry id. */
  def maxFileID = execQuery(maxFileIdS) {_.long("fileid")} head

  /** @return The numerically highest entry creation time stamp. */
  def maxCreateTime = execQuery(maxCreateTimeS) {_.long("createTime")} head
  
  def contains(print: TimeSizePrint) : Boolean =
    execQuery(matchesForPrintS){_ long 1}.head > 0

  /** @return The matching data entry ID if any. */
  def fileId(print: TimeSizePrintHash) : Option[Long] =
    execQuery(fileIdS, print.time, print.size, print.print, print.hash)(_ long 1)
    .headOnly

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
      val result = dbcon.connection.createStatement executeQuery "SELECT value FROM RepositoryInfo WHERE key = 'database version';"
      if (!result.next) throw new IllegalStateException("No database version entry in RepositoryInfo table")
      val dbversion = result getString "value"
      if (dbversion != "1.0") throw new IllegalStateException("Database version " + dbversion + " not supported")
      true
    } catch {
      case e: SQLSyntaxErrorException =>
        if(!e.getMessage.contains("object not found")) throw e
        false
    }
  }
  
  // EVENTUALLY add/remove constraints independently of database creation
  // e.g. ALTER TABLE DATAINFO ADD CONSTRAINT NoNegativeSize CHECK (size >= 0);
  def createTables(dbcon: DBConnection, settings: DBSettings /* FIXME remove? */, fsSettings: FSSettings) : Unit = {
    logger info "creating SQL tables"
    val connection = dbcon.connection
    
    // NOTES on SQL syntax used: A PRIMARY KEY constraint is equivalent to a
    // UNIQUE constraint on one or more NOT NULL columns. Only one PRIMARY KEY
    // can be defined in each table.
    // Source: http://hsqldb.org/doc/2.0/guide/guide.pdf
    
    // JOIN is the short form for INNER JOIN.
    
    // TODO check for needed indexes
    
/* for debugging purposes etc in external SQL tools

      CREATE CACHED TABLE DataInfo (
        id     BIGINT PRIMARY KEY,
        length BIGINT NOT NULL,
        print  BIGINT NOT NULL,
        hash   VARBINARY(16) NOT NULL,
        method INTEGER DEFAULT 0 NOT NULL
      , CHECK (length >= 0)
      , CHECK (method = 0 OR method = 1)
      , UNIQUE (length, print, hash)
      );

      CREATE CACHED TABLE TreeEntries (
        id          BIGINT PRIMARY KEY,
        parent      BIGINT NOT NULL,
        name        VARCHAR(256) NOT NULL,
        time        BIGINT DEFAULT NULL,
        dataid      BIGINT DEFAULT NULL,
        deleted     BOOLEAN DEFAULT FALSE NOT NULL,
        deleteTime  BIGINT DEFAULT 0 NOT NULL,
        UNIQUE (parent, name, deleted, deleteTime)
      , FOREIGN KEY (parent) REFERENCES TreeEntries(id)
      , FOREIGN KEY (dataid) REFERENCES DataInfo(id)
      , CHECK (parent != id OR id = -1)
      , CHECK ((dataid is NULL) = (time is NULL))
      , CHECK (deleted OR deleteTime = 0)
      , CHECK ((id < 1) = (parent = -1))
      , CHECK (id > -2)
      , CHECK ((id = 0) = (name = ''))
      , CHECK ((id != -1) OR (name = '*'))
      , CHECK (id > 0 OR deleted = FALSE)
      );
      
      CREATE CACHED TABLE ByteStore (
        dataid BIGINT NULL,
        index  INTEGER NOT NULL,
        start  BIGINT NOT NULL,
        fin    BIGINT NOT NULL
      , UNIQUE (start)
      , UNIQUE (fin)
      , FOREIGN KEY (dataid) REFERENCES DataInfo(id)
      , CHECK (fin > start AND start >= 0)
      );
        
      CREATE CACHED TABLE RepositoryInfo (
        key   VARCHAR(32) PRIMARY KEY,
        value VARCHAR(256) NOT NULL
      );


ORACLE VERSION:

      CREATE TABLE DataInfo (
        id     NUMBER(20) PRIMARY KEY,
        length NUMBER(20) NOT NULL,
        print  NUMBER(20) NOT NULL,
        hash   RAW(16) NOT NULL,
        method INTEGER DEFAULT 0 NOT NULL
      , CHECK (length >= 0)
      , CHECK (method = 0 OR method = 1)
      , UNIQUE (length, print, hash)
      );

      CREATE TABLE TreeEntries (
        id          NUMBER(20) PRIMARY KEY,
        parent      NUMBER(20) NOT NULL,
        name        VARCHAR(256) NOT NULL,
        time        NUMBER(20) DEFAULT NULL,
        dataid      NUMBER(20) DEFAULT NULL,
        deleted     char DEFAULT 0 NOT NULL,
        check (deleted in(0,1))
      , FOREIGN KEY (parent) REFERENCES TreeEntries(id),
      , FOREIGN KEY (dataid) REFERENCES DataInfo(id)
      );
      // TODO continue with constraints

      CREATE TABLE ByteStore (
        dataid NUMBER(20) NULL,
        part   INTEGER NOT NULL,
        begin  NUMBER(20) NOT NULL,
        end    NUMBER(20) NOT NULL
      );
      
      TODO index -> part
      TODO start -> begin (Oracle) fin -> end (consistency)
      
 */
    
    
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
    val zeroByteHash = fsSettings.hashProvider.getHashDigester getDigest
    val zeroBytePrint = fsSettings.printCalculator calculate RandomAccessInput.empty
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
        deleted     BOOLEAN DEFAULT FALSE NOT NULL,     // needed to avoid race conditions (e.g., delete & add child) TODO ???
        deleteTime  BIGINT DEFAULT 0 NOT NULL,          // timestamp if marked deleted, else 0
        UNIQUE (parent, name, deleted, deleteTime)      // unique TreeEntries only
        - constraints -
      );
      """,
      if (!settings.enableConstraints) "" else """
      , FOREIGN KEY (parent) REFERENCES TreeEntries(id) // reference integrity of parent
      , FOREIGN KEY (dataid) REFERENCES DataInfo(id)    // reference integrity of data info pointer
      , CHECK (parent != id OR id = -1)                 // no self references (except for root's parent)
      , CHECK ((dataid is NULL) = (time is NULL))       // timestamp iff data is present
      , CHECK (deleted OR deleteTime = 0)               // defined deleted status
      , CHECK ((id < 1) = (parent = -1))                // root's and root's parent's parent is -1
      , CHECK (id > -2)                                 // regular TreeEntries must have a positive id
      , CHECK ((id = 0) = (name = ''))                  // root's name is "", no other empty names
      , CHECK ((id != -1) OR (name = '*'))              // root's parent's name is "*"
      , CHECK (id > 0 OR deleted = FALSE)               // can't delete root nor root's parent
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
    execUpdate(connection, "INSERT INTO RepositoryInfo (key, value) VALUES ( 'hash algorithm', ? );", fsSettings.hashProvider.name)
    execUpdate(connection, "INSERT INTO RepositoryInfo (key, value) VALUES ( 'print algorithm', ? );", fsSettings.printCalculator.name)
    execUpdate(connection, "INSERT INTO RepositoryInfo (key, value) VALUES ( 'print length', ? );", fsSettings.printCalculator.length toString)
  }

}