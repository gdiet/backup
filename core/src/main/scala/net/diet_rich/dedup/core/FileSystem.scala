// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.data.DataBackendSlice
import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.util.{Equal, RichString}

trait FileSystem extends TreeInterface with StoreInterface with Lifecycle

object FileSystem {
  val ROOTID = TreeEntryID(0)
  val ROOTPARENTID = TreeEntryID(-1)
  val ROOTENTRY = TreeEntry(ROOTID, ROOTPARENTID, Path.ROOTNAME, None, None, None)
  val PRINTSIZE = 8192

  type Diagnostics = FileSystem with sql.SessionSlice with StoreSettingsSlice

  trait BasicPart
    extends StoreLogic
    with Tree
    with DataHandlerPart
    with sql.TablesPart
    with FreeRangesPart
    with data.DataStorePart { _: StoreSettingsSlice with DataBackendSlice with data.DataSettingsSlice with data.BlockSizeSlice => }

  implicit class FileSystemUtilities(val fs: FileSystem) extends AnyVal {
    def firstChild(parent: TreeEntryID, name: String): Option[TreeEntry] = fs.children(parent, name).headOption
    def firstChildren(parent: TreeEntryID): List[TreeEntry] = fs.children(parent).groupBy(_.name).values.flatMap(_.headOption).toList
    def dataEntry(id: TreeEntryID): Option[DataEntry] = dataid(id) flatMap fs.dataEntry
    def dataid(id: TreeEntryID): Option[DataEntryID] = fs entry id flatMap (_.data)
    def change(e: TreeEntry): Option[TreeEntry] = fs change (e.id, e.parent, e.name, e.changed, e.data, e.deleted)
    import Path.{ROOTPATH, SEPARATOR}
    def firstEntryWithWildcards(path: Path): Option[TreeEntry] = if (path == ROOTPATH) Some(ROOTENTRY) else {
      // FIXME make sure handling of the separator requirement is consistent
      require(path.value startsWith SEPARATOR, s"Path <$path> is not root and does not start with '${SEPARATOR}'")
      val parts = path.value split SEPARATOR drop 1
      parts.foldLeft(Option(ROOTENTRY)) {
        case (None, _) => None
        case (Some(node), pathElement) =>
          val childNameRegexp = pathElement.preparedForRegexpMatch
          firstChildren(node.id).toList.sortBy(_.name).reverse find (_.name matches childNameRegexp)
      }
    }

    def path(id: TreeEntryID): Option[Path] = {
      if (id === ROOTID) Some(Path.ROOTPATH) else {
        fs.entry(id) flatMap {entry => path(entry.parent) map (_ / entry.name)}
      }
    }
  }
}
