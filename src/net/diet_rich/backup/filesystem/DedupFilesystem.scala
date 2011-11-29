// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.filesystem

import net.diet_rich.util.io.{InputStream,BasicOutputStream}

class DedupFilesystem(val db: DBInterface) extends Filesystem[DedupEntry] {
  override def roots : Iterable[DedupEntry] =
    Iterable(entry(""))
  override def entry(path: String) : DedupEntry =
    new DedupEntry(this, path)
}

class DedupEntry(fs: DedupFilesystem, override val path: String) extends Entry[DedupEntry] {
  private def entry(path: String) : DedupEntry =
    new DedupEntry(fs, path)
  override def name : String = 
    path.split("/").last
  override def parent : Option[DedupEntry] =
    if (path == "/") None else Some(entry(path.split("/").mkString("/")))
  override def rename(newName: String) : Either[IOSignal, DedupEntry] =
    throw new UnsupportedOperationException // FIXME
  override def move(newParent: DedupEntry) : Either[IOSignal, DedupEntry] =
    throw new UnsupportedOperationException // FIXME
  override def delete : Option[IOSignal] =
    throw new UnsupportedOperationException // FIXME
  override def deleteAll : Option[IOSignal] =
    throw new UnsupportedOperationException // FIXME
  override def isFile: Boolean =
    fs.db.entryForPath(path).map(_.typ == FileType).getOrElse(false)
  override def isDir: Boolean =
    fs.db.entryForPath(path).map(_.typ == DirType).getOrElse(false)
    
  // directory
  override def makedirs : Option[IOSignal] =
    throw new UnsupportedOperationException // FIXME
  override def children : Either[IOSignal, Iterable[DedupEntry]] =
    throw new UnsupportedOperationException // FIXME
    
  // file
  override def size : Either[IOSignal, Long] =
    throw new UnsupportedOperationException // FIXME
  override def time : Either[IOSignal, Long] =
    throw new UnsupportedOperationException // FIXME
  override def input : Either[IOSignal, InputStream] =
    throw new UnsupportedOperationException // FIXME
  override def output : Either[IOSignal, BasicOutputStream] =
    throw new UnsupportedOperationException // FIXME
}

trait DBInterface {
  def entryForPath(path: String) : Option[DBEntry]
//  def entryForID(id: Long) : Option[DBEntry]
}

sealed trait EntryType
case object DirType extends EntryType
case object FileType extends EntryType

// root has name "" and path "/"
trait DBEntry {
  def name : String
  def typ : EntryType
}
