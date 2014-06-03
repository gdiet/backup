// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import org.specs2.SpecificationWithJUnit

import net.diet_rich.dedup.core.values._

class StoreRestoreSpec extends SpecificationWithJUnit { def is = s2"""
${"Tests for storing and restoring data".title}

Simple store and subsequent restore should be possible $storeRestore
  """

  private def withEmptyFileSystem[T] (f: FileSystem => T) = InMemoryDatabase.withDB { database =>
    val tables = new SQLTables(database)
    val data = new FileSystemData(tables, new DataSettings { override def blocksize = Size(100) }) with InMemoryDataBackend
    f(new FileSystem(data, new StoreSettings{}) with FileSystemTree with SQLTables.Component)
  }

  def storeRestore = withEmptyFileSystem { fileSystem =>
    val source = ???
    fileSystem storeUnchecked (FileSystem ROOTID, "child", source, Time(0))
    todo
  }

}
