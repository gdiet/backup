// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.vals._
import net.diet_rich.util.io.BytesSource
import net.diet_rich.util.vals._

trait Backend {
  def contains(size: Size, print: Print): Boolean
  def dataEntryFor(size: Size, print: Print, hash: Hash): Option[DataEntryID]
  def addTreeEntry(parent: TreeEntryID, name: String, time: Time, dataid: DataEntryID)
  def storeData(print: Print, source: BytesSource): DataEntryID
  def storeData(print: Print, hash: Hash, source: BytesSource): DataEntryID
}
