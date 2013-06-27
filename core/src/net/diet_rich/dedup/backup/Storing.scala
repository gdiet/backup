// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.vals._
import net.diet_rich.dedup.vals.Hash.BytesSource
import net.diet_rich.util.vals._

trait Storing {
  type EntryMetadata = {
    def name: String
    def time: Time
    def size: Size
    def print: Print
  }
  type Database = {
    def addEntry(parent: TreeEntryID, metadata: EntryMetadata, hash: Hash, dataid: DataEntryID)
  }
  type Datastore = {
    def store(source: BytesSource): DataEntryID
  }
  type Resource = EntryMetadata {
    def content: BytesSource
  }

  val database: Database
  val datastore: Datastore
  val hashAlgorithm: String
  
  def storeNewFile(file: Resource, parent: TreeEntryID) = {
    val (hash, dataid) = Hash.filter(hashAlgorithm)(file content)(datastore store)
    database addEntry (parent, file, hash, dataid)
  }
}
