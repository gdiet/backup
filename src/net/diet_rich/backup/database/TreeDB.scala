package net.diet_rich.backup.database

import net.diet_rich.backup.algorithm._
import net.diet_rich.util.sql._
import java.sql.Connection

trait TreeDB {
  /** @return The ID of the child if it could be created. */
  def createAndGetId(parentId: Long, name: String): Either[CreateFailedCause, Long]
  /** @return The node's complete data information if any. */
  def fullDataInformation(id: Long): Option[FullDataInformation]
  /** Does nothing if no such node. */
  def setData(id: Long, data: Option[DataInformation]): Option[UpdateFailedCause]
}

trait BasicTreeDB extends TreeDB {
  implicit def connection: Connection
  
  protected val maxEntryId =
    SqlDBUtil.readAsAtomicLong("SELECT MAX(id) FROM TreeEntries")
  
  protected val addEntry = 
    prepareUpdate("INSERT INTO TreeEntries (id, parent, name) VALUES (?, ?, ?)")
  
  protected val queryFullDataInformation = prepareQuery(
    "SELECT time, length, print, hash, dataid FROM TreeEntries JOIN DataInfo " +
    "ON TreeEntries.dataid = DataInfo.id AND TreeEntries.id = ?"
  )
  
  protected val changeData = 
    prepareUpdate("UPDATE TreeEntries SET time = ?, dataid = ? WHERE id = ?")
}

trait SafeTreeDB extends BasicTreeDB {
  override def createAndGetId(parentId: Long, name: String): Either[CreateFailedCause, Long] = {
    val id = maxEntryId incrementAndGet()
    addEntry(id, parentId, name) match {
      case 1 => Right(id)
      case rows => Left(CreateFailedCause("tree insert returned %s rows instead of 1".format(rows)))
    }
  }
  
  override final def fullDataInformation(id: Long): Option[FullDataInformation] =
    queryFullDataInformation(id)(
      q => FullDataInformation(q long 1, q long 2, q long 3, q bytes 4, q long 5)
    ).nextOptionOnly

  override def setData(id: Long, data: Option[DataInformation]): Option[UpdateFailedCause] =
    (data match {
      case Some(data) => changeData(data.time, data.dataid, id)
      case None => changeData(0, None, id)
    }) match {
      case 1 => None
      case rows => Some(UpdateFailedCause(""))
    }
}

trait UnsafeDelayedWriteTreeDB extends SafeTreeDB {
  def executeDelayedWrite: (=> Unit) => Unit
  def registerBackupProblem: BackupProblemCause => Unit
  
  override final def createAndGetId(parentId: Long, name: String): Either[CreateFailedCause, Long] = {
    val id = maxEntryId incrementAndGet()
    executeDelayedWrite {
      val rows = addEntry(id, parentId, name)
      if (rows != 1) registerBackupProblem(CreateFailedCause("tree insert returned %s rows instead of 1".format(rows)))
    }
    Right(id)
  }

  override final def setData(id: Long, data: Option[DataInformation]): Option[UpdateFailedCause] = {
    executeDelayedWrite {
	  super.setData(id, data).foreach(registerBackupProblem(_))
    }
    None
  }
  
  
/*
  protected val addEntry = 
    prepareUpdate("INSERT INTO TreeEntries (id, parent, name, time, dataid) VALUES (?, ?, ?, ?, ?);")
  /** can be overridden to run in a different thread. */
  protected def doAddEntry(id: Long, parent: Long, name: String, time: Long, data: Long): Unit = 
    addEntry(id, parent, name, time, data)
  override final def create(parent: Long, name: String, time: Long, data: Long): Long = {
    val id = maxEntryId incrementAndGet()
    doAddEntry(id, parent, name, time, data)
    id
  }
*/
  
}