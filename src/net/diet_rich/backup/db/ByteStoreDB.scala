// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.db

import java.sql.Connection
import net.diet_rich.util.Executor
import net.diet_rich.util.sql._

case class DataRange(start: Long, fin: Long) extends Ordered[DataRange] {
  def length = fin - start
  def withLength(length: Long) = copy(fin = start + length)
  def withOffset(offset: Long) = copy(start = start + offset)
  override def compare(that: DataRange): Int = that.start compare start
}

trait ByteStoreDB {
  /** @return the data for the entry. */
  def read(id: Long): Iterable[DataRange]
  /** Write a data blocks entry.
   *  @param id             Data id of the entry to write
   *  @param writeFunction  The write function is provided data ranges as input. It
   *                        is called repeatedly until it returns a written data 
   *                        size 0, signaling that there is no more data to write.
   */
  def write(id: Long)(writeFunction: DataRange => Long): Unit
}

object ByteStoreDB {
  def standardDB(implicit connection: Connection): ByteStoreDB =
    new ByteStoreSqlDB
    
  def deferredWriteDB(sqlExecutor: Executor)(implicit connection: Connection): ByteStoreDB =
    new ByteStoreSqlDB {
      protected override def doInsert(dataid: Long, index: Int, start: Long, fin: Long): Unit = 
        sqlExecutor(super.doInsert(dataid, index, start, fin))
    }
}

class ByteStoreSqlDB(implicit connection: Connection) extends ByteStoreDB { import ByteStoreSqlDB._
  
  protected object FreeRanges {
    private val startOfFreeArea = execQuery(connection, "SELECT MAX(fin) FROM ByteStore;")(_ long 1) next
    private val queue = new scala.collection.mutable.PriorityQueue[DataRange]()

    // insert ranges for gaps in queue
    private val gapStarts = execQuery(connection, idxStart( idxFin(
      "SELECT b1.fin FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.fin = b2.start WHERE b2.start IS NULL ORDER BY b1.fin"
    )))(_ long 1).filterNot(_ == startOfFreeArea)
    private val gapEndsAndDataStart = execQuery(connection, idxStart( idxFin(
      "SELECT b1.start FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.start = b2.fin WHERE b2.fin IS NULL ORDER BY b1.start"
    )))(_ long 1)
    private val dataStart = gapEndsAndDataStart.nextOption;
    
    // insert space between 0 and data start if any
    dataStart.filterNot(_ == 0).foreach(e => queue.enqueue(DataRange(0, e)))
    // insert gaps
    gapStarts.zip(gapEndsAndDataStart).foreach(e => queue.enqueue(DataRange(e._1, e._2)))
    // insert upper free area
    queue.enqueue(DataRange(startOfFreeArea, Long.MaxValue))

    def next: DataRange = queue.synchronized {
      val range = queue.dequeue()
      if (range.fin == Long.MaxValue) {
        queue.enqueue(range.withOffset(blockSize))
        range.withLength(blockSize)
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
  protected def doInsert(dataid: Long, index: Int, start: Long, fin: Long): Unit = 
    insertEntry(dataid, index, start, fin)
  override def write(dataid: Long)(writeFunction: DataRange => Long): Unit = {
    @annotation.tailrec
    def writeStep(index: Int): Unit = {
      val range = FreeRanges.next
      val stored = writeFunction(range)
      if (stored > 0) {
        doInsert(dataid, index, range.start, range.start + stored)
        // if the range was not fully used, re-enqueue the remainder
        if (stored < range.length) FreeRanges.enqueue(range.withOffset(stored))
        writeStep(index+1)
      }
      // if the range was not used up, re-enqueue it
      else FreeRanges.enqueue(range)
    }
    writeStep(0)
  }
}

object ByteStoreSqlDB {
  protected val blockSize = 100000000

  // EVENTUALLY, it would be good to look for illegal overlaps:
  // SELECT * FROM ByteStore b1 JOIN ByteStore b2 ON b1.start < b2.fin AND b1.fin > b2.fin
  // Illegal overlaps should be ignored during free space detection

  // EVENTUALLY, it would be good to look for orphan ByteStore entries.
  // Emit warning and free the space?

  def createTable(connection: Connection) : Unit = {
    // dataid: set to 0 for data chunks not needed anymore? (decide EVENTUALLY)
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
}