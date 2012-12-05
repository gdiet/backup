package net.diet_rich.backup.database

import net.diet_rich.util.sql._
import java.sql.Connection

trait DataInfoDB {
  /** @return true if at least one matching data entry is stored. */
  def hasMatch(size: Long, print: Long): Boolean
  /** @return The data id if a matching data entry is stored. */
  def findMatch(size: Long, print: Long, hash: Array[Byte]): Option[Long]
  /** @throws Exception if the entry was not created correctly. */
  def createDataEntry(dataid: Long, size: Long, print: Long, hash: Array[Byte]): Unit
}

trait BasicDataInfoDB extends DataInfoDB {
  implicit def connection: Connection
  
  protected final val checkPrint = 
    prepareQuery("SELECT COUNT(*) FROM DataInfo WHERE length = ? AND print = ?")
  override final def hasMatch(size: Long, print: Long): Boolean =
    checkPrint(size, print)(_.long(1) > 0).next

  protected final val findEntry = 
    prepareQuery("SELECT id FROM DataInfo WHERE length = ? AND print = ? AND hash = ?")
  override final def findMatch(size: Long, print: Long, hash: Array[Byte]) : Option[Long] =
    // NOTE: only the first of possibly multiple query results is used
    findEntry(size, print, hash)(_ long 1).nextOption

  protected final val addDataEntry = 
    prepareUpdate("INSERT INTO DataInfo (id, length, print, hash, method) VALUES (?, ?, ?, ?, ?)")
  override final def createDataEntry(dataid: Long, size: Long, print: Long, hash: Array[Byte]): Unit =
    addDataEntry(dataid, size, print, hash, 0) match {
      case 1 =>
      case n => throw new IllegalStateException("DataInfo: Insert entry returned %s rows instead of 1".format(n))
    }
    
}
