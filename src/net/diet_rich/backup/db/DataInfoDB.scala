// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.db

import java.sql.Connection
import net.diet_rich.util.sql._
import net.diet_rich.backup.HashCalcInput
import net.diet_rich.util.Executor

case class DataInfo (length: Long, print: Long, hash: Array[Byte], method: Int)

/** Basic data info db interface used for data access. */
trait BaseDataInfoDB {
  /** @return the data info, None if no such entry. */
  def readOption(id: Long) : Option[DataInfo]
  /** @return ID for the new data info. */
  def create(info: DataInfo) : Long
  def update(id: Long, info: DataInfo): Unit
  /** @return TRUE if a matching print is in the data info db. */
  def hasMatchingPrint(size: Long, print: Long) : Boolean
  /** @return matching id, None if no such entry. */
  def findMatch(size: Long, print: Long, hash: Array[Byte]) : Option[Long]
}

/** Convenience features for the data info db interface. */
trait DataInfoDB extends BaseDataInfoDB {
  /** @return the data info (exception if no such entry). */
  def read(id: Long): DataInfo = readOption(id) get
  /** @return ID for the new empty data info. */
  def create: Long = create(DataInfo(0, 0, Array(), 0))
}

object DataInfoDB {
  def standardDB(implicit connection: Connection): DataInfoDB =
    new DataInfoSqlDB with DataInfoDB
    
  def deferredWriteDB(sqlExecutor: Executor)(implicit connection: Connection): DataInfoDB =
    new DataInfoSqlDB with DataInfoDB {
      protected override def doUpdate(id: Long, info: DataInfo): Unit = 
        sqlExecutor(super.doUpdate(id, info))
      protected override def doCreate(id: Long, info: DataInfo): Unit = 
        sqlExecutor(super.doCreate(id, info))
    }
}

class DataInfoSqlDB(protected implicit val connection: Connection) extends BaseDataInfoDB {
  protected val maxEntryId = SqlDBUtil.readAsAtomicLong("SELECT MAX(id) FROM DataInfo;")

  protected val readEntry = 
    prepareQuery("SELECT length, print, hash, method FROM DataInfo WHERE id = ?;")
  override def readOption(id: Long): Option[DataInfo] =
    readEntry(id)(
      result => DataInfo(result long 1, result long 2, result bytes 3, result int 4)
    ) nextOption // NOTE: only the first of possibly multiple query results is used
  
  protected val insertNewEntry =
    prepareUpdate("INSERT INTO DataInfo (id, length, print, hash, method) VALUES (?, ?, ?, ?, ?);")
  protected def doCreate(id: Long, info: DataInfo): Unit = 
    insertNewEntry(id, info length, info print, info hash, info method)
  override def create(info: DataInfo): Long = {
    val id = maxEntryId incrementAndGet()
    doCreate(id, info)
    id
  }
  
  protected val updateEntry =
    prepareUpdate("UPDATE DataInfo SET length = ?, print = ?, hash = ?, method = ? WHERE id = ?;")
  protected def doUpdate(id: Long, info: DataInfo): Unit = 
    updateEntry(info length, info print, info hash, info method, id)
  override def update(id: Long, info: DataInfo): Unit =
    doUpdate(id, info)
    
  protected val checkPrint = 
    prepareQuery("SELECT COUNT(*) FROM DataInfo WHERE length = ? AND print = ?;")
  override def hasMatchingPrint(size: Long, print: Long) : Boolean =
    checkPrint(size, print)(_.long(1) > 0) next

  protected val findEntry = 
    prepareQuery("SELECT id FROM DataInfo WHERE length = ? AND print = ? AND hash = ?;")
  override def findMatch(size: Long, print: Long, hash: Array[Byte]) : Option[Long] =
    // NOTE: only the first of possibly multiple query results is used
    findEntry(size, print, hash)(_ long 1) nextOption
}

object DataInfoSqlDB {
  def createTable(connection: Connection, zeroByteHash: Array[Byte], zeroBytePrint: Long) : Unit = {
    // length: uncompressed entry size
    // method: store method (0 = PLAIN, 1 = DEFLATE, 2 = LZMA?)
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
    execUpdate(connection, "INSERT INTO DataInfo (id, length, print, hash, method) VALUES ( 0, 0, ?, ?, 0);", zeroBytePrint, zeroByteHash)
  }
  
  // used as index usage markers
  def idxDuplicates[T](t : T) = t
  def idxFastPrint[T](t : T) = t
  
  def cleanupDuplicatesFromTree(connection: Connection) = {
    val changeTreeDataEntries = TreeSqlDB idxDataid
      prepareUpdate("UPDATE TreeEntries SET dataid = ? WHERE dataid = ?;")(connection)
    duplicateEntries(connection) foreach { case (id1, id2) =>
      changeTreeDataEntries(id1, id2)
    }
  }
  
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
  
}
