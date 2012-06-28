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

case class DataRange(start: Long, fin: Long) extends Ordered[DataRange] {
  def length = fin - start
  def reduce(length: Long) = copy(fin = start + length)
  def offset(offset: Long) = copy(start = start + offset)
  override def compare(that: DataRange): Int = that.start compare start
}

trait ByteStoreDB {
  /** @return the data for the entry. */
  def read(id: Long): Iterable[DataRange]
  /** Write a data blocks entry.
   *  @param id             Data id of the entry to write
   *  @param source         The write function is provided data ranges as input. It
   *                        is called repeatedly until it returns a written data 
   *                        size 0, signaling that there is no more data to write.
   */
  def write(id: Long)(source: DataRange => Long)
}

class ByteStoreSqlDB(protected val connection: Connection) extends ByteStoreDB with SqlDBCommon {
  protected implicit val con = connection
  
  protected object FreeRanges {
    private val startOfFreeArea = execQuery(connection, "SELECT MAX(fin) FROM ByteStore;")(_ long 1) headOnly
    private val queue = new scala.collection.mutable.PriorityQueue[DataRange]()

    // insert ranges for gaps in queue
    private val gapFins = execQuery(connection, idxStart( idxFin(
      "SELECT b1.fin FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.fin = b2.start WHERE b2.start IS NULL ORDER BY b1.fin"
    )))(result => result long 1).filterNot(_ == startOfFreeArea)
    private val gapStartsAndHead = execQuery(connection, idxStart( idxFin(
      "SELECT b1.start FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.start = b2.fin WHERE b2.fin IS NULL ORDER BY b1.start"
    )))(result => result long 1)
    private val dataStart = gapStartsAndHead.headOption;
    private val gapStarts = if (gapStartsAndHead.isEmpty) Iterator() else gapStartsAndHead
    
    dataStart.filterNot(_ == 0).foreach(e => queue.enqueue(DataRange(0, e)))
    gapFins.zip(gapStarts).foreach(e => queue.enqueue(DataRange(e._1, e._2)))

    // insert upper free area
    queue.enqueue(DataRange(startOfFreeArea, Long.MaxValue))

    def next: DataRange = queue.synchronized {
      val range = queue.dequeue()
      if (range.fin == Long.MaxValue) {
        queue.enqueue(range.offset(blockSize))
        range.reduce(blockSize)
      } else range
    }
    def enqueue(range: DataRange) =
      queue.synchronized { queue.enqueue(range) }
  }
  
  protected val readEntry = 
    prepareQuery("SELECT start, fin FROM ByteStore WHERE dataid = ? ORDER BY index;")
  override def read(id: Long): Iterable[DataRange] =
    readEntry(id){ result => DataRange(result long 1, result long 2) }.toSeq
  
  protected val insertEntry = 
    prepareUpdate("INSERT INTO ByteStore (dataid, index, start, fin) VALUES (?, ?, ?, ?);")
  override def write(id: Long)(source: DataRange => Long) = {
    @annotation.tailrec
    def writeStep(index: Int): DataRange = {
      val range = FreeRanges.next
      val stored = source(range)
      if (stored != 0)
        insertEntry(id, index, range.start, range.start + stored) match {
          case 1 => /* OK */
          case n => throwIllegalUpdateException("ByteStore", n, id)
        }
      if (stored != 0) writeStep(index+1) else DataRange(range.start + stored, range.fin)
    }
    FreeRanges.enqueue(writeStep(0))
  }
    
}

object ByteStoreSqlDB extends SqlDBObjectCommon {
  protected val blockSize = 100000000
  override val tableName = "ByteStore"

  // EVENTUALLY, it would be good to look for illegal overlaps:
  // SELECT * FROM ByteStore b1 JOIN ByteStore b2 ON b1.start < b2.fin AND b1.fin > b2.fin
  // Illegal overlaps should be ignored during free space detection

  // EVENTUALLY, it would be good to look for orphan ByteStore entries (in case the FOREIGN KEY constraint is not enabled).
  // Emit warning and free the space?
    
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
  
  def apply(connection: Connection) : ByteStoreSqlDB = new ByteStoreSqlDB(connection)
}