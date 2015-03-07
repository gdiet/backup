package net.diet_rich.dedup.core.meta

import org.specs2.Specification

class TreeSpec extends Specification with MetaMatchers { def is = s2"""
${"Tests for the file system tree".title}

The root node should be a directory $rootIsDirectory
A directory should be available in the tree even if its newly crested $createAndCheckDirectory
Looking up a path where only parts exist should yield None $pathWithoutTreeEntry
Create should throw an exception if a child with the name already exists $createExisting
Creating a child where a deleted child with the same name already exists should succeed $createReplacement
Creating paths should succeed even if they already exist partially $createPaths
"""

  private def withEmptyTree[T] (f: MetaBackend => T): T = {
    val sessionFactory = sql.Testutil.memoryDB
    sql.DBUtilities.createTables("MD5")(sessionFactory.session)
    f(new sql.SQLMetaBackend(sessionFactory))
  }

  def rootIsDirectory = withEmptyTree { tree =>
    tree.entries("") should contain(exactly(haveNoData))
  }

  def createAndCheckDirectory = withEmptyTree { tree =>
    val child = tree create (rootEntry.id, "child")
    tree.entries("/child") should contain(exactly(haveNoData and haveTheId(child id)))
  }

  def pathWithoutTreeEntry = withEmptyTree { tree =>
    tree create (rootEntry.id, "child")
    tree.entries("/child/doesNotExist") should beEmpty
  }

  def createExisting = withEmptyTree { tree =>
    tree create (rootEntry.id, "child")
    tree create (rootEntry.id, "child") should throwA[java.io.IOException]
  }

  def createReplacement = withEmptyTree { tree =>
    val child = tree create (rootEntry.id, "child")
    tree markDeleted child.id
    tree create (rootEntry.id, "child") should haveNoData
  }

  def createPaths = withEmptyTree { tree =>
    tree createWithPath "/some/path"
    tree createWithPath "/some/other/path"
    tree entries "/some" should haveSize(1)
  }
}
