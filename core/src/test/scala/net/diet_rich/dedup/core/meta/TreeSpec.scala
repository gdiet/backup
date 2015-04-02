package net.diet_rich.dedup.core.meta

import org.specs2.Specification

class TreeSpec extends Specification with MetaMatchers with MetaUtils { def is = s2"""
${"Tests for the file system tree".title}

The root node should be a directory $rootIsDirectory
A directory should be available in the tree even if its newly crested $createAndCheckDirectory
Looking up a path where only parts exist should yield None $pathWithoutTreeEntry
Create should throw an exception if a child with the name already exists $createExisting
Creating a child where a deleted child with the same name already exists should succeed $createReplacement
Creating paths should succeed even if they already exist partially $createPaths
"""

  def rootIsDirectory = withEmptyMetaBackend { tree =>
    tree.entries("") should contain(exactly(haveNoData))
  }

  def createAndCheckDirectory = withEmptyMetaBackend { tree =>
    val child = tree create (rootEntry.id, "child")
    tree.entries("/child") should contain(exactly(haveNoData and haveTheId(child)))
  }

  def pathWithoutTreeEntry = withEmptyMetaBackend { tree =>
    tree create (rootEntry.id, "child")
    tree.entries("/child/doesNotExist") should beEmpty
  }

  def createExisting = withEmptyMetaBackend { tree =>
    tree create (rootEntry.id, "child")
    tree create (rootEntry.id, "child") should throwA[java.io.IOException]
  }

  def createReplacement = withEmptyMetaBackend { tree =>
    val child = tree create (rootEntry.id, "child")
    tree markDeleted child
    val newChild = tree create (rootEntry.id, "child")
    tree.entry(child).get should haveNoData
  }

  def createPaths = withEmptyMetaBackend { tree =>
    tree createWithPath "/some/path"
    tree createWithPath "/some/other/path"
    tree entries "/some" should haveSize(1)
  }
}
