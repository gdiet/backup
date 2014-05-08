// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import scala.concurrent.Future

import net.diet_rich.dedup.core.values._

trait FileSystem extends MetaFileSystem with DataFileSystem

object FileSystem {
  val ROOTNAME = ""
  val SEPARATOR = "/"
  val ROOTID = TreeEntryID(0)
  val ROOTPARENTID = TreeEntryID(-1)
  val ROOTENTRY = TreeEntry(ROOTID, ROOTPARENTID, ROOTNAME, None, None, None)
}

trait MetaFileSystem {
  protected val meta: SQLTables

  def createDir(parent: TreeEntryID, name: String): Future[TreeEntryID] = meta.createTreeEntry(parent, name)
}

trait DataFileSystem {

}
