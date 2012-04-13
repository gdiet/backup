package net.diet_rich.sb

import java.sql.Connection
import net.diet_rich.util.ScalaThreadLocal
import net.diet_rich.util.sql._
import net.diet_rich.util.Configuration._
import java.util.concurrent.atomic.AtomicLong
import java.sql.PreparedStatement
import java.sql.SQLException

case class DataInfo (length: Long, print: Long, hash: Array[Byte], method: Int)

trait DataInfoDB {
  def read(id: Long) : DataInfo = readOption(id) get
  def readOption(id: Long) : Option[DataInfo]
  def write(info: DataInfo) : Long
}

class DataInfoSqlDB(connection: Connection) extends DataInfoDB {
  protected val maxEntryId: AtomicLong = new AtomicLong(
    execQuery(connection, "SELECT MAX(id) FROM DataInfo;")(_ long 1) headOnly
  )
  
  override def readOption(id: Long) : Option[DataInfo] =
    execQuery(connection, "SELECT length, print, hash, method FROM DataInfo WHERE id = ?;", id)(
      result => DataInfo(result long 1, result long 2, result bytes 3, result int 4)
    ) headOption

  protected def prepare(statement: String) : ScalaThreadLocal[PreparedStatement] =
    ScalaThreadLocal(connection prepareStatement statement, statement)
    
  protected val insertNewEntry_ = prepare("INSERT INTO DataInfo (id, length, print, hash, method) VALUES (?, ?, ?, ?, ?);")
    
  override def write(info: DataInfo) : Long = {
    val id = maxEntryId incrementAndGet()
    try { execUpdate(insertNewEntry_, id, info length, info print, info hash, info method) match {
      case 1 => id
      case n => throw new IllegalStateException("Write: Unexpected %s times update for id %s" format(n, id))
    } } catch { case e: SQLException => maxEntryId compareAndSet(id, id-1); throw e }
  }
}

object DataInfoSqlDB {
  // FIXME detect duplicates and orphan entries

  def apply(connection: Connection) : DataInfoSqlDB = new DataInfoSqlDB(connection)
  
  def createTables(connection: Connection) : Unit = {
    // length: uncompressed entry size
    // method: store method (0 = PLAIN, 1 = DEFLATE, 2 = LZMA?)
    val algorithm = RepositoryInfoDB.read(connection).string("hash algorithm")
    val zeroByteHash = HashProvider.digest(algorithm).digest
    execUpdate(connection, """
      CREATE CACHED TABLE DataInfo (
        id     BIGINT PRIMARY KEY,
        length BIGINT NOT NULL,
        print  BIGINT NOT NULL,
        hash   VARBINARY(?) NOT NULL,
        method INTEGER NOT NULL
      );
    """, zeroByteHash.size);
    execUpdate(connection, "INSERT INTO DataInfo (id, length, print, hash, method) VALUES ( 0, 0, ?, ?, 0);", PrintDigester.zeroBytePrint, zeroByteHash)
  }
  
  def dropTables(connection: Connection) : Unit =
    execUpdate(connection, "DROP TABLE DataInfo IF EXISTS;")
  
  protected val constraints = List(
    "NoNegativeLength CHECK (length >= 0)",
    "ValidMethod CHECK (method = 0 OR method = 1)"
  )

  def addConstraints(connection: Connection) : Unit =
    constraints foreach(constraint => execUpdate(connection, "ALTER TABLE DataInfo ADD CONSTRAINT " + constraint))
  
  def removeConstraints(connection: Connection) : Unit =
    constraints foreach(constraint => execUpdate(connection, "ALTER TABLE DataInfo DROP CONSTRAINT " + constraint.split(" ").head))
}