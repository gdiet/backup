// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import java.sql.Connection
import net.diet_rich.util.io._
import net.diet_rich.util.vals._

class BackupFileSystem(val dig: Digesters, protected val ds: net.diet_rich.dedup.datastore.DataStore2)(implicit val connection: Connection)
extends TreeTable with RespectDeleted with TreeTableUtils with DataInfoTable with ByteStoreDB

class BackupFileSystemIgnoreDeleted(val dig: Digesters, protected val ds: net.diet_rich.dedup.datastore.DataStore2)(implicit val connection: Connection)
extends TreeTable with IgnoreDeleted with TreeTableUtils with DataInfoTable with ByteStoreDB

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
