// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.util.sql._
import net.diet_rich.util.vals._

trait DataInfoDB {
  implicit val connection: WrappedConnection
  
  /** @return true if at least one matching data entry is stored. */
  def hasMatch(size: Size, print: Print): Boolean =
    checkPrint(size.value, print.value)(_.long(1) > 0).next
  protected val checkPrint = 
    prepareQuery("SELECT COUNT(*) FROM DataInfo WHERE length = ? AND print = ?;")
  
  /** @return The data id if a matching data entry is stored. */
  def findMatch(size: Size, print: Print, hash: Hash): Option[DataEntryID] =
    // NOTE: only the first of possibly multiple query results is used
    findEntry(size.value, print.value, hash.value)(p => DataEntryID(p long 1)).nextOption
  protected val findEntry = 
    prepareQuery("SELECT id FROM DataInfo WHERE length = ? AND print = ? AND hash = ?;")
  
  /** @throws Exception if the entry was not created correctly. */
  def createDataEntry(dataid: DataEntryID, size: Size, print: Print, hash: Hash): Unit =
    insertNewEntry(dataid.value, size.value, print.value, hash.value)
  protected val insertNewEntry =
    prepareSingleRowUpdate("INSERT INTO DataInfo (id, length, print, hash) VALUES (?, ?, ?, ?)")
}

object DataInfoDB {
  def createTable(zeroByteHash: Hash, zeroBytePrint: Print)(implicit connection: WrappedConnection) : Unit = {
    // length: uncompressed entry size
    // method: store method (0 = PLAIN, 1 = DEFLATE, 2 = LZMA??)
    execUpdate(net.diet_rich.util.Strings normalizeMultiline f"""
      CREATE TABLE DataInfo (
        id     BIGINT PRIMARY KEY,
        length BIGINT NOT NULL,
        print  BIGINT NOT NULL,
        hash   VARBINARY(${zeroByteHash.value.size}%d) NOT NULL,
        method INTEGER DEFAULT 0 NOT NULL
      );
    """)
    execUpdate("CREATE INDEX idxDataInfoDuplicates ON DataInfo(length, print, hash)")
    execUpdate("CREATE INDEX idxDataInfoFastPrint ON DataInfo(length, print)")
    execUpdate("INSERT INTO DataInfo (id, length, print, hash) VALUES (0, 0, ?, ?)", zeroBytePrint.value, zeroByteHash.value)
  }
  
//  def cleanupDuplicatesFromTree(connection: Connection) = {
//    val changeTreeDataEntries = TreeSqlDB idxDataid
//      prepareUpdate("UPDATE TreeEntries SET dataid = ? WHERE dataid = ?;")(connection)
//    duplicateEntries(connection) foreach { case (id1, id2) =>
//      changeTreeDataEntries(id1, id2)
//    }
//  }
//  
//  protected def duplicateEntries(connection: Connection) : Iterator[(Long, Long)] =
//    execQuery(connection, idxDuplicates(
//      """SELECT d1.id, d2.id FROM DataInfo d1 JOIN DataInfo d2
//         ON d1.length = d2.length
//         AND d1.print = d2.print
//         AND d1.hash = d2.hash
//         AND d1.id < d2.id;"""
//    ))(
//      result => ((result long 1, result long 2))
//    )
//
//  def deleteOrphansHereAndInByteStore(connection: Connection) : Int = {
//    execUpdate(connection, TreeSqlDB idxDataid
//      """DELETE FROM ByteStore WHERE dataid IN (
//           SELECT DISTINCT id FROM DataInfo
//           LEFT OUTER JOIN TreeEntries ON DataInfo.id = TreeEntries.dataid
//           WHERE TreeEntries.dataid is NULL
//         );"""
//    )
//    execUpdate(connection, TreeSqlDB idxDataid
//      """DELETE FROM DataInfo WHERE id IN (
//           SELECT DISTINCT id FROM DataInfo
//           LEFT OUTER JOIN TreeEntries ON DataInfo.id = TreeEntries.dataid
//           WHERE TreeEntries.dataid is NULL
//           AND DataInfo.id != 0
//         );"""
//    )
//  }
}