package net.diet_rich.backup.algorithm

import net.diet_rich.util.io.Reader
import net.diet_rich.backup.database.TreeDB

trait DataInformation {
  def time: Long
  def dataid: Long
}

case class FullDataInformation (
  override val time: Long,
  size: Long,
  print: Long,
  hash: Array[Byte],
  override val dataid: Long
) extends DataInformation

trait BackupProblemCause
case class CreateFailedCause(technicalExplanation: String) extends BackupProblemCause
case class UpdateFailedCause(technicalExplanation: String) extends BackupProblemCause

trait BackupFileSystem extends TreeDB {
  override def createAndGetId(parentId: Long, name: String): Either[CreateFailedCause, Long]
//  /** @return The child's entry ID if any. */
//  def childId(parentId: Long, name: String): Option[Long]
  override def fullDataInformation(id: Long): Option[FullDataInformation]
  override def setData(id: Long, data: Option[DataInformation]): Option[UpdateFailedCause]
//  /** @return The matching data id if any. */
//  def dataid(size: Long, print: Long, hash: Array[Byte]): Option[Long]
//  /** @return <code>true</code> if a matching entry is in storage. */
//  def hasMatch(size: Long, print: Long): Boolean
//  
//  def storeAndGetDataIdAndSize(reader: Reader): (Long, Long)
//  def storeAndGetDataId(bytes: Array[Byte], size: Long): Long
// 
}
