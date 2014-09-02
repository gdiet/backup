// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.data.DataBackendSlice
import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.util.Equal

trait FileSystem extends TreeInterface with StoreInterface with Lifecycle

object FileSystem {
  val ROOTID = TreeEntryID(0)
  val ROOTPARENTID = TreeEntryID(-1)
  val ROOTENTRY = TreeEntry(ROOTID, ROOTPARENTID, Path.ROOTNAME, None, None, None)
  val PRINTSIZE = 8192

  trait BasicPart
    extends StoreLogic
    with Tree
    with DataHandlerPart
    with sql.TablesPart
    with FreeRangesPart
    with data.DataStorePart { _: StoreSettingsSlice with DataBackendSlice with data.DataSettingsSlice => }

  implicit class FileSystemUtilities(val fs: FileSystem) extends AnyVal {
    def firstChild(parent: TreeEntryID, name: String): Option[TreeEntry] = fs.children(parent, name).headOption
    def firstChildren(parent: TreeEntryID): List[TreeEntry] = fs.children(parent).groupBy(_.name).values.flatMap(_.headOption).toList
    def dataEntry(id: TreeEntryID): Option[DataEntry] = dataid(id) flatMap fs.dataEntry
    def dataid(id: TreeEntryID): Option[DataEntryID] = fs entry id flatMap (_.data)

    def path(id: TreeEntryID): Option[Path] = {
      if (id === ROOTID) Some(Path.ROOTPATH) else {
        fs.entry(id) flatMap {entry => path(entry.parent) map (_ / entry.name)}
      }
    }
  }
}
