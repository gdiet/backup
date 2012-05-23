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
import ByteStoreSqlDB._
import net.diet_rich.util.Bytes

case class DataRange(start: Long, fin: Long)

object DataRange {
  val startOrdering: Ordering[DataRange] = new Ordering[DataRange] {
    override def compare(a: DataRange, b: DataRange) = a.start compareTo b.start
  }
  val startComparator = new java.util.Comparator[DataRange] {
    override def compare(a: DataRange, b: DataRange) = a.start compareTo b.start
  }
}

trait ByteStoreDB {
  /** @return the data for the entry. */
  def read(id: Long): Iterable[DataRange]
  /** Write a data blocks entry.
   *  @param id             Data id of the entry to write
   *  @param sizeEstimate   Estimated size in bytes of the entry to write
   *  @param source         The write function is provided data ranges as input.
   *                        It is called repeatedly until it returns a data range
   *                        other than its input, signaling that there is no more
   *                        data to write.
   */
  def write(id: Long, sizeEstimate: Long)(source: DataRange => DataRange)
}

class ByteStoreSqlDB(protected val connection: Connection) extends ByteStoreDB with SqlDBCommon {
  protected implicit val con = connection
  
  protected var startOfFreeArea = execQuery(connection, "SELECT MAX(fin) FROM ByteStore;")(_ long 1) headOnly
  protected val nextFreeRanges = new scala.collection.mutable.PriorityQueue[DataRange]()(DataRange.startOrdering)
  protected def nextFreeRange: DataRange =
    nextFreeRanges.synchronized {
      if (nextFreeRanges.isEmpty)
        for (n <- 0 until 10) {
          nextFreeRanges enqueue DataRange(startOfFreeArea, startOfFreeArea + 100000000)
          startOfFreeArea = startOfFreeArea + 100000000
        }
      nextFreeRanges dequeue
    }
  protected def enqueueFreeRange(range: DataRange) =
    nextFreeRanges.synchronized { nextFreeRanges.enqueue(range) }
  
  execQuery(connection, idxStart( idxFin(
    "SELECT b1.start FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.start = b2.fin WHERE b2.fin IS NULL"
  )))(result => result long 1).toList filterNot (_ == 0)

  protected val readEntry = 
    prepareQuery("SELECT start, fin FROM ByteStore WHERE dataid = ? ORDER BY index;")
  override def read(id: Long): Iterable[DataRange] =
    readEntry(id){ result => DataRange(result long 1, result long 2) }.toSeq
  
  override def write(id: Long, sizeEstimate: Long)(source: DataRange => DataRange) = {
    throw new UnsupportedOperationException
  }
    
}

object ByteStoreSqlDB extends SqlDBObjectCommon {
  override val tableName = "ByteStore"

  // EVENTUALLY, it would be good to look for illegal overlaps:
  // SELECT * FROM ByteStore b1 JOIN ByteStore b2 ON b1.start < b2.fin AND b1.fin > b2.fin
  // Illegal overlaps should be ignored during free space detection

  // EVENTUALLY, it would be good to look for orphan ByteStore entries (in case the FOREIGN KEY constraint is not enabled).
  // Emit warning and free the space?
    
  // Find gaps
  // a) start without matching fin
  // SELECT * FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 on b1.start = b2.fin where b2.fin is null
  // b) fin without matching start
  // SELECT * FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 on b1.fin = b2.start where b2.start is null

  // Find highest entry
  // SELECT MAX(fin) FROM ByteStore
    
  def createTable(connection: Connection, repoSettings: StringMap) : Unit = {
    val zeroByteHash = HashProvider.digester(repoSettings).digest
    // dataid: set to 0 for data chunks not needed anymore
    // index: data part index (starts at 0)
    // start: data part start position
    // fin: data part end position + 1
    execUpdate(connection, """
      CREATE CACHED TABLE ByteStore (
        dataid BIGINT NOT NULL,
        index  INTEGER NOT NULL,
        start  BIGINT NOT NULL,
        fin    BIGINT NOT NULL
      );
    """);
    execUpdate(connection, "CREATE INDEX idxStart ON ByteStore(start);")
    execUpdate(connection, "CREATE INDEX idxFin ON ByteStore(fin);")
    execUpdate(connection, "CREATE INDEX idxData ON ByteStore(dataid);")
  }
  
  // used as index usage markers
  def idxStart[T](t : T) = t
  def idxFin[T](t : T) = t
  def idxData[T](t : T) = t
  
  override protected val internalConstraints = List(
    "UniqueStart UNIQUE (start)",
    "UniqueFin UNIQUE (fin)",
    "ValidPositions CHECK (fin > start AND start >= 0)"
  )
  
  override protected val externalConstraints = List(
    "DataReference FOREIGN KEY (dataid) REFERENCES DataInfo(id)"
  )
  
  def apply(connection: Connection) : DataInfoSqlDB = new DataInfoSqlDB(connection)
}