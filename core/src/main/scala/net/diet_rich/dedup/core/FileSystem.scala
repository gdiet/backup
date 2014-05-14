// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import java.io.IOException
import scala.concurrent.Future

import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.core.values.Implicits._

trait FileSystem extends MetaFileSystem with DataFileSystem

object FileSystem {
  val ROOTID = TreeEntryID(0)
  val ROOTPARENTID = TreeEntryID(-1)
  val ROOTENTRY = TreeEntry(ROOTID, ROOTPARENTID, Path.ROOTNAME, None, None, None)
}

trait MetaFileSystem {
  import FileSystem._

  protected val sqlTables: SQLTables

  def childrenWithDeleted(parent: TreeEntryID): List[TreeEntry] = sqlTables treeChildren parent
  def children(parent: TreeEntryID): List[TreeEntry] = childrenWithDeleted(parent) filter (_.deleted isEmpty)

  def createUnchecked(parent: TreeEntryID, name: String, time: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntryID =
    sqlTables createTreeEntry (parent, name, time, dataid)
  def create(parent: TreeEntryID, name: String, time: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntryID = sqlTables inWriteContext {
    children(parent) find (_.name == name) match {
      case Some(entry) => throw new IOException(s"entry $entry already exists")
      case None => createUnchecked(parent, name, time, dataid)
    }
  }
  def createWithPath(path: Path, time: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntryID = sqlTables inWriteContext {
    val elements = path.elements
    if(elements.size == 0) throw new IOException("can't create the root entry")
    val parent = elements.dropRight(1).foldLeft(ROOTID) { (node, childName) =>
      children(node) filter (_.name == childName) match {
        case Nil => createUnchecked(node, childName, time, dataid)
        case List(entry) => entry.id
        case entries => throw new IOException(s"ambiguous path; §entries")
      }
    }
    create(parent, elements.last, time, dataid)
  }

  def entries(path: Path): List[TreeEntry] =
    path.elements.foldLeft(List(ROOTENTRY)) { (node, childName) =>
      node flatMap (children(_) filter (_.name == childName))
    }
}

trait DataFileSystem {

}
