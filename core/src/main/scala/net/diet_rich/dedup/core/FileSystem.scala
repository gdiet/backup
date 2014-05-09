// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import scala.concurrent.Future

import net.diet_rich.dedup.core.values._

trait FileSystem extends MetaFileSystem with DataFileSystem

object FileSystem {
  val ROOTNAME = ""
  val ROOTPATH = Path("")
  val SEPARATOR = "/"
  val ROOTID = TreeEntryID(0)
  val ROOTPARENTID = TreeEntryID(-1)
  val ROOTENTRY = TreeEntry(ROOTID, ROOTPARENTID, ROOTNAME, None, None, None)
}

trait MetaFileSystem {
  import FileSystem._

  protected val meta: EnrichedSQLTables

  def createDir(parent: TreeEntryID, name: String): Future[TreeEntryID] = meta createTreeEntry (parent, name)

  def treeEntry(path: Path): Option[TreeEntry] = if (path == ROOTPATH) meta treeEntry ROOTID else {
    assume(path.value startsWith SEPARATOR, s"Path <$path> is not root and does not start with '$SEPARATOR'")
    val parts = path.value split SEPARATOR drop 1
    parts.foldLeft(Option(ROOTENTRY)) { (node, childName) =>
      node flatMap (node => meta treeChildren node find (_.name == childName))
    }
  }
}

trait DataFileSystem {

}
