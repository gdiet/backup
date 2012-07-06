// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.fdfs

import java.sql.Connection
import net.diet_rich.util.Configuration._
import net.diet_rich.util.sql._
import net.diet_rich.sb.HashProvider
import net.diet_rich.sb.PrintDigester

case class DataInfo (length: Long, print: Long, hash: Array[Byte], method: Int)

trait DataInfoDB {
  /** @return the data info, throw exception if no such entry. */
  final def read(id: Long) : DataInfo = readOption(id) get
  /** @return the data info, None if no such entry. */
  def readOption(id: Long) : Option[DataInfo]
  /** @return TRUE if a matching print is in the data info db. */
  def hasMatchingPrint(size: Long, print: Long) : Boolean
  /** @return matching id, None if no such entry. */
  def findMatch(size: Long, print: Long, hash: Array[Byte]) : Option[Long]
  /** @return ID for a new data info entry. */
  def reserveID : Long
  /** @return ID for the new data info. */
  def create(id: Long, info: DataInfo) : Unit
}

object DataInfoSqlDB {
  
  // NOTES on SQL syntax used: A PRIMARY KEY constraint is equivalent to a
  // UNIQUE constraint on one or more NOT NULL columns. Only one PRIMARY KEY
  // can be defined in each table.
  // (Source: http://hsqldb.org/doc/2.0/guide/guide.pdf)
  def createTable(connection: Connection, hashAlgorithm: String) : Unit = {
    // length: uncompressed entry size
    // method: store method (0 = PLAIN, 1 = DEFLATE, 2 = LZMA?)
    val zeroByteHash = HashProvider.digester(hashAlgorithm).digest
    execUpdate(connection, """
      CREATE CACHED TABLE DataInfo (
        id     BIGINT PRIMARY KEY,
        length BIGINT NOT NULL,
        print  BIGINT NOT NULL,
        hash   VARBINARY(%d) NOT NULL,
        method INTEGER NOT NULL
      );
    """ format zeroByteHash.size);
    execUpdate(connection, "CREATE INDEX idxDuplicates ON DataInfo(length, print, hash);")
    execUpdate(connection, "CREATE INDEX idxFastPrint ON DataInfo(length, print);")
    execUpdate(connection, "INSERT INTO DataInfo (id, length, print, hash, method) VALUES ( 0, 0, ?, ?, 0);", PrintDigester.zeroBytePrint, zeroByteHash)
  }
  
  // used as index usage markers
  def idxDuplicates[T](t : T) = t
  def idxFastPrint[T](t : T) = t
  
  def apply(connection: Connection) : DataInfoSqlDB = new DataInfoSqlDB()(connection)
  
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
           AND DataInfo.id != 0
         );"""
    )
  }
  
  def setupDB(connection: Connection) : DataInfoDB = new DataInfoSqlDB()(connection)
  def setupDeferredInsertDB(connection: Connection, executor: SqlDBCommon.Executor) : DataInfoDB =
    new DeferredInsertDataInfoDB(connection, executor)
}

protected[fdfs] class DataInfoSqlDB(protected implicit val connection: Connection) extends DataInfoDB with SqlDBCommon {
  import DataInfoSqlDB._
  
  protected val maxEntryId = readAsAtomicLong("SELECT MAX(id) FROM DataInfo;")

  protected val readEntry = 
    idxFastPrint(prepareQuery("SELECT length, print, hash, method FROM DataInfo WHERE id = ?;"))
  override def readOption(id: Long) : Option[DataInfo] =
    readEntry(id)(
      result => DataInfo(result long 1, result long 2, result bytes 3, result int 4)
    ) headOption // NOTE: the query may yield multiple results - only the first is used

  protected val checkPrint = 
    prepareQuery("SELECT COUNT(*) FROM DataInfo WHERE length = ? AND print = ?;")
  override def hasMatchingPrint(size: Long, print: Long) : Boolean =
    checkPrint(size, print)( result => (result long 1) > 0 ) head

  protected val findEntry = 
    prepareQuery("SELECT id FROM DataInfo WHERE length = ? AND print = ? AND hash = ?;")
  override def findMatch(size: Long, print: Long, hash: Array[Byte]) : Option[Long] =
    // NOTE: the query may yield multiple results - only the first is used
    findEntry(size, print, hash)(result => result long 1) headOption

  override def reserveID : Long = maxEntryId incrementAndGet()
  
  protected val insertNewEntry =
    prepareUpdate("INSERT INTO DataInfo (id, length, print, hash, method) VALUES (?, ?, ?, ?, ?);")
  protected def doInsertNewEntry(id: Long, info: DataInfo): Unit =
    insertNewEntry(id, info length, info print, info hash, info method)
  override final def create(id: Long, info: DataInfo) : Unit =
    doInsertNewEntry(id, info)
}

protected[fdfs] class DeferredInsertDataInfoDB (
      connection: Connection,
      executor: SqlDBCommon.Executor
    ) extends DataInfoSqlDB()(connection) {
  import net.diet_rich.util.closureToRunnable
  
  protected override def doInsertNewEntry(id: Long, info: DataInfo): Unit =
    executor.execute { insertNewEntry(id, info length, info print, info hash, info method) }
}

