// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import java.sql.Connection
import java.sql.SQLException
import net.diet_rich.util.Configuration._
import net.diet_rich.util.EventSource
import net.diet_rich.util.Events
import net.diet_rich.util.sql._

case class DataInfo (length: Long, print: Long, hash: Array[Byte], method: Int)

trait DataInfoDB {
  /** @return the data info (exception if no such entry). */
  final def read(id: Long) : DataInfo = readOption(id) get
  /** @return the data info, None if no such entry. */
  def readOption(id: Long) : Option[DataInfo]
  /** @return ID for the new data info. */
  def write(info: DataInfo) : Long
}

class DataInfoSqlDB(protected val connection: Connection) extends DataInfoDB with SqlDBCommon {
  implicit val con = connection
  
  protected val maxEntryId = readAsAtomicLong("SELECT MAX(id) FROM DataInfo;")

  protected val readEntry = 
    prepareQuery("SELECT length, print, hash, method FROM DataInfo WHERE id = ?;")
  override def readOption(id: Long) : Option[DataInfo] =
    readEntry(id)(
      result => DataInfo(result long 1, result long 2, result bytes 3, result int 4)
    ) headOption // NOTE: the query may yield multiple results - only the first is used

  protected val entryInsertES = new EventSource[(Long, DataInfo)]
  def entryInsertEvent : Events[(Long, DataInfo)] = entryInsertES
  
  protected val insertNewEntry =
    prepareUpdate("INSERT INTO DataInfo (id, length, print, hash, method) VALUES (?, ?, ?, ?, ?);")
  override def write(info: DataInfo) : Long = {
    val id = maxEntryId incrementAndGet()
    try { insertNewEntry(id, info length, info print, info hash, info method) match {
      case 1 => entryInsertES emit (id -> info); id
      case n => throw new IllegalStateException("Write: Unexpected %s times update for id %s" format(n, id))
    } } catch { case e: SQLException => maxEntryId compareAndSet(id, id-1); throw e }
  }
}

object DataInfoSqlDB extends SqlDBObjectCommon {
  override val tableName = "DataInfo"
  
  // NOTES on SQL syntax used: A PRIMARY KEY constraint is equivalent to a
  // UNIQUE constraint on one or more NOT NULL columns. Only one PRIMARY KEY
  // can be defined in each table.
  // (Source: http://hsqldb.org/doc/2.0/guide/guide.pdf)
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
    execUpdate(connection, "CREATE INDEX idxFastPrint ON DataInfo(length, print);")
    execUpdate(connection, "INSERT INTO DataInfo (id, length, print, hash, method) VALUES ( 0, 0, ?, ?, 0);", PrintDigester.zeroBytePrint, zeroByteHash)
  }
  
  // used as index usage markers
  def idxDuplicates[T](t : T) = t
  def idxFastPrint[T](t : T) = t
  
  override protected val internalConstraints = List(
    "NoNegativeLength CHECK (length >= 0)",
    "ValidMethod CHECK (method = 0 OR method = 1)"
  )
  
  def apply(connection: Connection) : DataInfoSqlDB = new DataInfoSqlDB(connection)
  
  def cleanupDuplicatesFromTree(connection: Connection) = {
    val changeTreeDataEntries = TreeSqlDB idxDataid
      prepareUpdate("UPDATE TreeEntries SET dataid = ? WHERE dataid = ?;")(connection)
    duplicateEntries(connection) foreach { case (id1, id2) =>
      changeTreeDataEntries(id1, id2)
    }
  }
  
  // JOIN is the short form for INNER JOIN.
  protected def duplicateEntries(connection: Connection) : Iterator[(Long, Long)] =
    execQuery(connection, idxDuplicates(
      """SELECT d1.id, d2.id FROM DataInfo d1 JOIN DataInfo d2
         ON d1.length = d2.length
         AND d1.print = d2.print
         AND d1.hash = d2.hash
         AND d1.id < d2.id;"""
    ))(
      result => ((result long 1, result long 2))
    )

  def deleteOrphansHereAndInByteStore(connection: Connection) : Int = {
    execUpdate(connection, TreeSqlDB idxDataid
      """DELETE FROM ByteStore WHERE dataid IN (
           SELECT DISTINCT id FROM DataInfo
           LEFT OUTER JOIN TreeEntries ON DataInfo.id = TreeEntries.dataid
           WHERE TreeEntries.dataid is NULL
         );"""
    )
    execUpdate(connection, TreeSqlDB idxDataid
      """DELETE FROM DataInfo WHERE id IN (
           SELECT DISTINCT id FROM DataInfo
           LEFT OUTER JOIN TreeEntries ON DataInfo.id = TreeEntries.dataid
           WHERE TreeEntries.dataid is NULL
         );"""
    )
  }
}