// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

case class TreeEntry (
  id: TreeEntryID,
  parent: TreeEntryID,
  name: String,
  changed: Option[Time],
  data: Option[DataEntryID],
  deleted: Option[Time]
) {
  def isDirectory = data.isEmpty
  def isFile = data.isDefined
}
