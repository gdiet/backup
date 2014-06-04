// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values._

class FileSystem(
  protected val data: FileSystemData,
  protected val storeSettings: StoreSettings
) extends FileSystemTree with FileSystemStoreLogic {
  protected val sqlTables = data.sqlTables
}

object FileSystem {
  val ROOTID = TreeEntryID(0)
  val ROOTPARENTID = TreeEntryID(-1)
  val ROOTENTRY = TreeEntry(ROOTID, ROOTPARENTID, Path.ROOTNAME, None, None, None)
  val PRINTSIZE = 8192
}
