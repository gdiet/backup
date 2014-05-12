// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import scala.concurrent.Future

import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.core.values.Implicits._

trait FileSystem extends MetaFileSystem with DataFileSystem

object FileSystem {
  val ROOTID = TreeEntryID(0)
  val ROOTPARENTID = TreeEntryID(-1)
  val ROOTENTRY = TreeEntry(ROOTID, ROOTPARENTID, Path.ROOTNAME, None, None, None)
}

trait MetaFileSystem {
  import Path._
  import FileSystem._

  protected val sqlTables: SQLTables

  def children(parent: TreeEntryID): List[TreeEntry] = sqlTables treeChildren parent

  def createDir(parent: TreeEntryID, name: String): Future[TreeEntryID] = sqlTables createTreeEntry (parent, name)

  def getOrMakeDir(path: Path): Future[TreeEntryID] = ??? // FIXME continue if (path == ROOTPATH) Future successful ROOTID else {
//    val parts = path.value.split(SEPARATOR).drop(1)
//    parts.foldLeft(ROOTID) {(node, childName) =>
//      val childOption = children(node).find(_.name == childName)
//      childOption map(_.id) getOrElse createAndGetId(node, childName, NodeType.DIR)
//    }
//  }

  def treeEntry(path: Path): Option[TreeEntry] = if (path == ROOTPATH) sqlTables treeEntry ROOTID else {
    val parts = path.value split SEPARATOR drop 1
    parts.foldLeft(Option(ROOTENTRY)) { (node, childName) =>
      node flatMap (children(_) find (_.name == childName))
    }
  }
}

trait DataFileSystem {

}
