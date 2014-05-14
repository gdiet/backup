// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import java.io.IOException
import org.specs2.SpecificationWithJUnit

import net.diet_rich.dedup.core.values.Path

class MetaFileSystemSpec extends SpecificationWithJUnit with ValueMatchers { def is = s2"""
${"Tests for the directory tree".title}

The root node should be a directory $rootIsDirectory
A directory should be available in the tree even if its newly crested $createAndCheckDirectory
Looking up a path where only parts exist yields None $pathWithoutTreeEntry
Create throws an exception if a child with the name already exists $createExisting
getOrMakeDir $todo
  """

  private def withEmptyFileSystem[T] (f: MetaFileSystem => T) = InMemoryDatabase.withDB { database =>
    f(new MetaFileSystem { override val sqlTables = new SQLTables(database) })
  }

  def createExisting = withEmptyFileSystem { fileSystem =>
    fileSystem create (FileSystem ROOTID, "child")
    fileSystem create (FileSystem ROOTID, "child") should throwA[IOException]
  }

  def pathWithoutTreeEntry = withEmptyFileSystem { fileSystem =>
    fileSystem create (FileSystem ROOTID, "child")
    fileSystem.entries(Path("/child/doesNotExist")) should beEmpty
  }

  def createAndCheckDirectory = withEmptyFileSystem { fileSystem =>
    val childId = fileSystem create (FileSystem ROOTID, "child")
    fileSystem.entries(Path("/child")) should contain(exactly(beADirectory and haveTheId(childId)))
  }

  def rootIsDirectory = withEmptyFileSystem {
    _.entries(Path("")) should contain(exactly(beADirectory))
  }
}
