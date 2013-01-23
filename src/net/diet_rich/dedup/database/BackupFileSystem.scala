// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.util.io._
import net.diet_rich.util.vals._
import net.diet_rich.util.sql.WrappedConnection

class BackupFileSystem(val dig: Digesters, protected val ds: net.diet_rich.dedup.datastore.DataStore)(implicit val connection: WrappedConnection)
extends TreeDB with TreeDBUtils with DataInfoDB with ByteStoreDB

case class FullDataInformation (
  time: Time,
  size: Size,
  print: Print,
  hash: Hash,
  dataid: Option[DataEntryID]
)

trait Digesters extends PrintDigester {
  def filterHash[ReturnType](source: ByteSource)(processor: ByteSource => ReturnType): (Hash, ReturnType)
}
