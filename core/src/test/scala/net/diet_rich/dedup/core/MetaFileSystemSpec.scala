// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import org.specs2.SpecificationWithJUnit

import net.diet_rich.dedup.core.values.Path

class MetaFileSystemSpec extends SpecificationWithJUnit with ValueMatchers { def is = s2"""
${"Tests for the directory tree".title}

The root node should be a directory $rootIsDirectory
A newly created directory should be available in the tree $createAndCheckDirectory
Looking up a path where only parts exist yields None $pathWithoutTreeEntry
  """

  private def withEmptyFileSystem[T] (f: MetaFileSystem => T) = InMemoryDatabase.withDB { database =>
    f(new MetaFileSystem { override val sqlTables = new SQLTables(database) })
  }

  def rootIsDirectory = withEmptyFileSystem {
    _.treeEntry(Path("")) should beSomeDirectory
  }

  def createAndCheckDirectory = withEmptyFileSystem { fileSystem =>
    val childId = waitFor(fileSystem createDir (FileSystem ROOTID, "child"))
    fileSystem.treeEntry(Path("/child")) should (beSomeDirectory and haveSomeId(childId))
  }

  def pathWithoutTreeEntry = withEmptyFileSystem { fileSystem =>
    waitFor(fileSystem createDir (FileSystem ROOTID, "child"))
    fileSystem.treeEntry(Path("/child/doesNotExist")) should beNone
  }
}
