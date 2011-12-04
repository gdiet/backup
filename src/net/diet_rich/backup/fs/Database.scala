// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.fs

/** Interface definition a database has to implement. */
trait Database {
  /** Get the entry for an ID if any. */
  def get(id: Long) : Option[DBEntry]
  /** Get a named child entry. */
  def get(id: Long, childName: String) : Option[DBEntry]
  /** Rename (but do not move) an entry, return it if successful. */
  def rename(id: Long, newName: String) : Option[DBEntry]
  /** Move (but do not rename) an entry, return it if successful. */
  def move(id: Long, newParent: Long) : Option[DBEntry]
  /** Delete an entry together with any children. Return the entry's last state if successful. */
  def delete(id: Long) : Option[DBEntry]
  /** Create a new directory, return it if successful. */
  def makeDir(id: Long, childName: String) : Option[DBEntry]
  /** Create a new 0 byte file, return it if successful. */
  def makeFile(id: Long, childName: String) : Option[DBEntry]
  /** Update an existing file, return it if successful. */
  def changeFile(id: Long, time: Long, data: Long) : Option[DBEntry]
}

/** Database utility methods. */
trait DatabaseAddons {
  self: Database =>

  final val rootID       = 0L
  final val rootName     = ""
  final val rootParentID = 0L

  /** Rename (but do not move) an entry. */
  final def rename(entry: DBEntry, newName: String) : Option[DBEntry] = rename (entry id, newName)
  
  /** Get the current state of an entry (which may have been deleted). */
  final def update[T <: DBEntry](entry: T) : Option[T] = get(entry id) .asInstanceOf[Option[T]]

  /** Get the entry for a path. */
  final def getPath(path: String) : Option[DBEntry] = {
    require((path equals "") || (path matches "/.*[^/]")) // starting but not ending with a slash
    (path split "/" tail) .foldLeft (get(rootID)) ((parent, name) =>
      parent flatMap( entry => get(entry parentID, name) )
    )
  }
}

sealed trait DBEntry {
  /** Root directory: 0 */
  val id: Long
  /** Root directory: "" */
  val name: String
  /** Root directory: 0 */
  val parentID: Long
  final val dir : Option[DBDir]  = this match { case dir:  DBDir  => Some(dir)  ; case _ => None }
  final val file: Option[DBFile] = this match { case file: DBFile => Some(file) ; case _ => None }
}

final case class DBDir(
  override val id: Long,
  override val name: String,
  override val parentID: Long,
  /** In SQL, children are stored only via the parent relation. */
  childIDs: List[Long]
) extends DBEntry

final case class DBFile(
  override val id: Long,
  override val name: String,
  override val parentID: Long,
  /** Last modified time stamp. */
  time: Long,
  /** Data ID needed to look up size, hash, and content, 0 for 0-byte entry. */
  data: Long
) extends DBEntry
