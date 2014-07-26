// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import java.io.IOException

import net.diet_rich.dedup.core.values.{DataEntryID, TreeEntryID, TreeEntry, Time, Path, Size}
import net.diet_rich.dedup.util.Equal

trait TreeInterface {
  def childrenWithDeleted(parent: TreeEntryID): List[TreeEntry]
  def children(parent: TreeEntryID): List[TreeEntry]
  def children(parent: TreeEntryID, name: String): List[TreeEntry]
  // TODO default in the interface or in the implementation?
  def createUnchecked(parent: TreeEntryID, name: String, changed: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntry
  def create(parent: TreeEntryID, name: String, changed: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntry
  def createWithPath(path: Path, changed: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntry
  def markDeleted(id: TreeEntryID, deletionTime: Option[Time] = Some(Time now())): Boolean
  def change(id: TreeEntryID, newParent: TreeEntryID, newName: String, newTime: Option[Time], newData: Option[DataEntryID]): Option[TreeEntry]
  def createOrReplace(parent: TreeEntryID, name: String, changed: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntry
  def sizeOf(id: DataEntryID): Option[Size]
  def entries(path: Path): List[TreeEntry]
}

trait Tree extends TreeInterface with sql.TablesPart {
  override final def childrenWithDeleted(parent: TreeEntryID): List[TreeEntry] = tables treeChildren parent
  override final def children(parent: TreeEntryID): List[TreeEntry] = childrenWithDeleted(parent) filter (_.deleted isEmpty)
  override final def children(parent: TreeEntryID, name: String): List[TreeEntry] = children(parent) filter (_.name === name)
  override final def markDeleted(id: TreeEntryID, deletionTime: Option[Time]): Boolean = tables markDeleted (id, deletionTime) // TODO can be written shorter using function values?
  override final def change(id: TreeEntryID, newParent: TreeEntryID, newName: String, newTime: Option[Time], newData: Option[DataEntryID]): Option[TreeEntry] = tables updateTreeEntry (id, newParent, newName, newTime, newData)
  override final def sizeOf(id: DataEntryID): Option[Size] = tables dataEntry id map (_.size)

  override final def createUnchecked(parent: TreeEntryID, name: String, changed: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntry =
    tables createTreeEntry (parent, name, changed, dataid)
  override final def create(parent: TreeEntryID, name: String, changed: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntry = tables inTransaction {
    children(parent) find (_.name === name) match {
      case Some(entry) => throw new IOException(s"entry $entry already exists")
      case None => createUnchecked(parent, name, changed, dataid)
    }
  }
  override final def createOrReplace(parent: TreeEntryID, name: String, changed: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntry = tables inTransaction {
    children(parent) find (_.name === name) match {
      case Some(entry) => change(entry id, parent, name, changed, dataid); entry
      case None => createUnchecked(parent, name, changed, dataid)
    }
  }
  override final def createWithPath(path: Path, changed: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntry = tables inTransaction {
    val elements = path.elements
    if(elements.size === 0) throw new IOException("can't create the root entry")
    val parent = elements.dropRight(1).foldLeft(FileSystem ROOTID) { (node, childName) =>
      children(node) filter (_.name === childName) match {
        case Nil => createUnchecked(node, childName, changed, dataid).id
        case List(entry) => entry.id
        case entries => throw new IOException(s"ambiguous path; §entries")
      }
    }
    create(parent, elements.last, changed, dataid)
  }

  override final def entries(path: Path): List[TreeEntry] =
    path.elements.foldLeft(List(FileSystem ROOTENTRY)) { (nodes, childName) =>
      nodes flatMap (node => children(node.id) filter (_.name === childName))
    }
}
