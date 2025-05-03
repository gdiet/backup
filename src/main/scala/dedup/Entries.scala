package dedup

import dedup.TreeEntry.*

import java.text.SimpleDateFormat
import java.util.Date

sealed trait TreeEntry { def id: Long; def parentId: Long; def name: String
  def deleted: Time
  def isDeleted: Boolean = deleted != NotDeleted
}
object TreeEntry:
  val NotDeleted = Time(0)
  val dateFormat = SimpleDateFormat("yyyy-MM-dd_HH-mm_ss")
  def dateString(time: Time) = dateFormat.format(Date(time.asLong))
  def apply(id: Long, parentId: Long, name: String, time: Time, dataId: Option[DataId]): TreeEntry =
    dataId.fold(DirEntry(id, parentId, name, time, NotDeleted))(FileEntry(id, parentId, name, time, NotDeleted, _))
  def includingDeleted(id: Long, parentId: Long, name: String, time: Time, deleted: Time, dataId: Option[DataId]): TreeEntry =
    dataId.fold(DirEntry(id, parentId, name, time, deleted))(FileEntry(id, parentId, name, time, deleted, _))

case class DirEntry (id: Long, parentId: Long, nameInDB: String, time: Time, deleted: Time)                 extends TreeEntry:
  def name: String =
    println(s"$nameInDB $deleted $isDeleted")
    if isDeleted then s"$nameInDB#del#${dateString(deleted)}" else nameInDB

case class FileEntry(id: Long, parentId: Long, nameInDB: String, time: Time, deleted: Time, dataId: DataId) extends TreeEntry:
  def name: String =
    println(s"$nameInDB $deleted $isDeleted")
    if isDeleted then s"$nameInDB#del#${dateString(deleted)}" else nameInDB

val root = DirEntry(0, 0, "", now, NotDeleted)
