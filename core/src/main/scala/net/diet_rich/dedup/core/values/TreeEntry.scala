// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

case class TreeEntry (
  id: TreeEntryID,
  parent: TreeEntryID,
  name: String,
  time: Option[Time],
  deleted: Option[Time],
  data: Option[DataEntryID]
)
