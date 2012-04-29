// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import java.sql.Connection
import net.diet_rich.util.ScalaThreadLocal
import net.diet_rich.util.sql._
import net.diet_rich.util.Configuration._
import java.util.concurrent.atomic.AtomicLong
import java.sql.PreparedStatement
import java.sql.SQLException
import df.DataInfo

trait DataInfoDB {
  def read(id: Long) : DataInfo = readOption(id) get
  def readOption(id: Long) : Option[DataInfo]
  def write(info: DataInfo) : Long
}

class DataInfoSqlDB(protected val connection: Connection) extends DataInfoDB with SqlDBCommon {
  
  protected val maxEntryId = readAsAtomicLong("SELECT MAX(id) FROM DataInfo;")
  
  override def readOption(id: Long) : Option[DataInfo] =
    execQuery(connection, "SELECT length, print, hash, method FROM DataInfo WHERE id = ?;", id)(
      result => DataInfo(result long 1, result long 2, result bytes 3, result int 4)
    ) headOption

  protected val insertNewEntry_ = prepare("INSERT INTO DataInfo (id, length, print, hash, method) VALUES (?, ?, ?, ?, ?);")
    
  override def write(info: DataInfo) : Long = {
    val id = maxEntryId incrementAndGet()
    try { execUpdate(insertNewEntry_, id, info length, info print, info hash, info method) match {
      case 1 => id
      case n => throw new IllegalStateException("Write: Unexpected %s times update for id %s" format(n, id))
    } } catch { case e: SQLException => maxEntryId compareAndSet(id, id-1); throw e }
  }
  
  // FIXME listener for possible orphans
}

object DataInfoSqlDB extends SqlDBObjectCommon {
  // NOTES on SQL syntax used: A PRIMARY KEY constraint is equivalent to a
  // UNIQUE constraint on one or more NOT NULL columns. Only one PRIMARY KEY
  // can be defined in each table.
  // (Source: http://hsqldb.org/doc/2.0/guide/guide.pdf)
  
  // JOIN is the short form for INNER JOIN.

  // FIXME cleanup method: delete orphans and duplicates

  override val tableName = "DataInfo"

  def cleanupDuplicates(connection: Connection) = {
//    execQuery(connection, idxDuplicates(
//      """SELECT id, length, print, hash, method FROM DataInfo d1
//         JOIN DataInfo d2
//         ON d1.length = d2.length
//         AND d1.print = d2.print
//         AND d1.hash = d2.hash
//         AND d1.id < d2.id;"""
//    ))(
//      result => ((result long 1) -> DataInfo(result long 2, result long 3, result bytes 4, result int 5))
//    ) toMap
    // FIXME needs TreeDataDB
    throw new UnsupportedOperationException
  }
  
  def duplicateEntries(connection: Connection) : Map[Long, DataInfo] =
    execQuery(connection, idxDuplicates(
      """SELECT id, length, print, hash, method FROM DataInfo d1
         JOIN DataInfo d2
         ON d1.length = d2.length
         AND d1.print = d2.print
         AND d1.hash = d2.hash
         AND d1.id < d2.id;"""
    ))(
      result => ((result long 1) -> DataInfo(result long 2, result long 3, result bytes 4, result int 5))
    ) toMap
    
  def orphanEntries(connection: Connection) : Map[Long, DataInfo] =
    execQuery(connection, TreeSqlDB.idxDataid(
      """SELECT DISTINCT id, length, print, hash, method FROM DataInfo
         LEFT OUTER JOIN TreeEntries ON DataInfo.id = TreeEntries.dataid
         WHERE TreeEntries.dataid is NULL;"""
    ))(
      result => ((result long 1) -> DataInfo(result long 2, result long 3, result bytes 4, result int 5))
    ) toMap

  def apply(connection: Connection) : DataInfoSqlDB = new DataInfoSqlDB(connection)
  
  def createTable(connection: Connection, repoSettings: StringMap) : Unit = {
    // length: uncompressed entry size
    // method: store method (0 = PLAIN, 1 = DEFLATE, 2 = LZMA?)
    val zeroByteHash = HashProvider.digester(repoSettings).digest
    execUpdate(connection, """
      CREATE CACHED TABLE DataInfo (
        id     BIGINT PRIMARY KEY,
        length BIGINT NOT NULL,
        print  BIGINT NOT NULL,
        hash   VARBINARY(?) NOT NULL,
        method INTEGER NOT NULL
      );
    """, zeroByteHash.size);
    execUpdate(connection, "CREATE INDEX idxDuplicates ON DataInfo(length, print, hash);")
    execUpdate(connection, "INSERT INTO DataInfo (id, length, print, hash, method) VALUES ( 0, 0, ?, ?, 0);", PrintDigester.zeroBytePrint, zeroByteHash)
  }
  
  def idxDuplicates[T](t : T) = t
  
  def dropTables(connection: Connection) : Unit =
    execUpdate(connection, "DROP TABLE DataInfo IF EXISTS;")
  
  protected val constraints = List(
    "NoNegativeLength CHECK (length >= 0)",
    "ValidMethod CHECK (method = 0 OR method = 1)"
  )
}