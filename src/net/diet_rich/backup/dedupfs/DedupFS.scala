// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.dedupfs

class DedupFS(private[dedupfs] val db: CachedDB) {
  def get(path: String) : Entry = new Entry(this, path)
}

class Entry(val fs: DedupFS, val path: String) {
  require(path.startsWith("/") || path.isEmpty())
  require(!path.endsWith("/"))
  
  def data: Option[EntryData] = {
    fs.db getPath path map (_ match {
      case dir: DBDir => new EntryData(
          id = dir.id,
          isDir = true
        )
      case _ => throw new UnsupportedOperationException
    })
  }
  
  def name: String = path split "/" last
  def parent: String = if (path equals "") "" else path substring (0, path.lastIndexOf('/'))
  
  def mkdir : Option[Signal] = fs.db mkdir (name, parent)
}

trait Signal

case object EntryExists extends Signal
case object ParentDoesNotExist extends Signal
case object ParentIsNotADir extends Signal

class EntryData(
  val id: Long,
  val isDir: Boolean,
  val size: Option[Long] = None,
  val time: Option[Long] = None
){
  def isFile : Boolean = ! isDir
}

//class Dir(val fs: DedupFS, val id: Long) extends Entry
