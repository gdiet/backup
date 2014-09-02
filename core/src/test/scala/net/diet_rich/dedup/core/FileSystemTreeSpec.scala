// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.sql.InMemoryDBPartWithTables
import net.diet_rich.dedup.core.values.Path
import org.specs2.SpecificationWithJUnit

class FileSystemTreeSpec extends SpecificationWithJUnit with TreeMatchers { def is = s2"""
${"Tests for the file system tree".title}

The root node should be a directory $rootIsDirectory
A directory should be available in the tree even if its newly crested $createAndCheckDirectory
Looking up a path where only parts exist should yield None $pathWithoutTreeEntry
Create should throw an exception if a child with the name already exists $createExisting
Creating a child where a deleted child with the same name already exists should succeed $createReplacement
  """

  private def withEmptyTree[T] (f: TreeInterface => T): T = {
    object tree extends Tree with InMemoryDBPartWithTables
    f(tree)
  }

  def createReplacement = withEmptyTree { tree =>
    val child = tree create (FileSystem ROOTID, "child")
    tree markDeleted child.id
    tree create (FileSystem ROOTID, "child") should beADirectory
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
    val child = tree create (FileSystem ROOTID, "child")
    tree.entries(Path("/child")) should contain(exactly(beADirectory and haveTheId(child id)))
  }

  def rootIsDirectory = withEmptyTree {
    _.entries(Path("")) should contain(exactly(beADirectory))
  }
}
