// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import java.io.InputStream
import java.sql.Connection
import net.diet_rich.util.Hash
import net.diet_rich.util.io._
import net.diet_rich.util.vals._
import scala.concurrent.Future

class BackupFileSystem(val dig: Digesters, protected val ds: net.diet_rich.dedup.datastore.DataStore2)(implicit val connection: Connection)
extends TreeDB with TreeDBQueries with TreeDBUtils with DataInfoDB with ByteStoreDB {
  // TODO move where this belongs
  def createFile(parent: TreeEntryID, name: String, data: InputStream, size: Long, time: Time = now): TreeEntry = {
    val dataid = createDataEntry(data, size)
    val id = createAndGetId(parent, name, NodeType.FILE, time, Some(dataid))
    TreeEntry(id, parent, name, NodeType.FILE, time, None, Some(dataid))
  }
  def createDataEntry(data: InputStream, size: Long): DataEntryID = {
    DataEntryID(0) // FIXME
  }
}

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
