// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values._

trait TreeInterface {
  def childrenWithDeleted(parent: TreeEntryID): List[TreeEntry]
  def children(parent: TreeEntryID): List[TreeEntry]
  def createUnchecked(parent: TreeEntryID, name: String, changed: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntryID
  def create(parent: TreeEntryID, name: String, changed: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntryID
  def createWithPath(path: Path, changed: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntryID
  def entries(path: Path): List[TreeEntry]
}
