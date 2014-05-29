// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import java.io.IOException

import net.diet_rich.dedup.core.FileSystem._
import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.core.values.Implicits.treeEntryToID

trait FileSystemTree { _: SQLTablesComponent =>

  def childrenWithDeleted(parent: TreeEntryID): List[TreeEntry] = sqlTables treeChildren parent
  def children(parent: TreeEntryID): List[TreeEntry] = childrenWithDeleted(parent) filter (_.deleted isEmpty)

  def createUnchecked(parent: TreeEntryID, name: String, changed: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntryID =
    sqlTables createTreeEntry (parent, name, changed, dataid)
  def create(parent: TreeEntryID, name: String, changed: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntryID = sqlTables inWriteContext {
    children(parent) find (_.name == name) match {
      case Some(entry) => throw new IOException(s"entry $entry already exists")
      case None => createUnchecked(parent, name, changed, dataid)
    }
  }
  def createWithPath(path: Path, changed: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntryID = sqlTables inWriteContext {
    val elements = path.elements
    if(elements.size == 0) throw new IOException("can't create the root entry")
    val parent = elements.dropRight(1).foldLeft(ROOTID) { (node, childName) =>
      children(node) filter (_.name == childName) match {
        case Nil => createUnchecked(node, childName, changed, dataid)
        case List(entry) => entry.id
        case entries => throw new IOException(s"ambiguous path; §entries")
      }
    }
    create(parent, elements.last, changed, dataid)
  }

  def entries(path: Path): List[TreeEntry] =
    path.elements.foldLeft(List(ROOTENTRY)) { (node, childName) =>
      node flatMap (children(_) filter (_.name == childName))
    }
}
