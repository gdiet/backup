// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import org.specs2.SpecificationWithJUnit

import net.diet_rich.dedup.core.values.Path

class FileSystemTreeSpec extends SpecificationWithJUnit with TreeMatchers { def is = s2"""
${"Tests for the file system tree".title}

The root node should be a directory $rootIsDirectory
A directory should be available in the tree even if its newly crested $createAndCheckDirectory
Looking up a path where only parts exist yields None $pathWithoutTreeEntry
Create throws an exception if a child with the name already exists $createExisting
  """

  private def withEmptyTree[T] (f: TreeInterface => T): T = {
    object tree extends TreeSlice with sql.InMemoryDatabaseSlice {
      sql.DBUtilities createTables 16
      sql.DBUtilities recreateIndexes
    }
    f(tree)
  }

  def createExisting = withEmptyTree { tree =>
    tree create (FileSystem ROOTID, "child")
    tree create (FileSystem ROOTID, "child") should throwA[java.io.IOException]
  }

  def pathWithoutTreeEntry = withEmptyTree { tree =>
    tree create (FileSystem ROOTID, "child")
    tree.entries(Path("/child/doesNotExist")) should beEmpty
  }

  def createAndCheckDirectory = withEmptyTree { tree =>
    val childId = tree create (FileSystem ROOTID, "child")
    tree.entries(Path("/child")) should contain(exactly(beADirectory and haveTheId(childId)))
  }

  def rootIsDirectory = withEmptyTree {
    _.entries(Path("")) should contain(exactly(beADirectory))
  }
}
