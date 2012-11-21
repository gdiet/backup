package net.diet_rich.backup.database

import net.diet_rich.util.sql._
import java.sql.Connection

trait DataInfoDB {
  /** @return true if at least one matching data entry is stored. */
  def hasMatch(size: Long, print: Long): Boolean
  /** @return The data id if a matching data entry is stored. */
  def findMatch(size: Long, print: Long, hash: Array[Byte]) : Option[Long]
}

trait BasicDataInfoDB extends DataInfoDB {
  implicit def connection: Connection
  
  protected val checkPrint = 
    prepareQuery("SELECT COUNT(*) FROM DataInfo WHERE length = ? AND print = ?")
  override def hasMatch(size: Long, print: Long): Boolean =
    checkPrint(size, print)(_.long(1) > 0) next

  protected val findEntry = 
    prepareQuery("SELECT id FROM DataInfo WHERE length = ? AND print = ? AND hash = ?;")
  override def findMatch(size: Long, print: Long, hash: Array[Byte]) : Option[Long] =
    // NOTE: only the first of possibly multiple query results is used
    findEntry(size, print, hash)(_ long 1) nextOption
    
}
