package net.diet_rich.dedupfs.metadata.sql

import org.specs2.Specification
import org.specs2.specification.AfterAll

import net.diet_rich.common._, test._
import net.diet_rich.dedupfs.metadata.TreeEntry

class TreeStandardOpsSpec extends Specification with AfterAll with TestsHelper with PackageHelper {
  def is = sequential ^ s2"""
General tests for the tree database, starting with a newly initialized database
  where child is TreeEntry(1, 0, "child", None, None):
  Initially, the root node has no children:
  ${eg{ db.treeChildrenOf(0) === List() }}
  The first child to be inserted gets the key 1:
  ${eg{ db.create(0, "child", None, None) === 1 }}
  The second child, inserted as child of the first child, gets the key 2:
  ${eg{ db.create(1, "grandChild", None, None) === 2 }}
  The root node now has exactly one child:
  ${eg{ db.treeChildrenOf(0) === List(child) }}
  There is no deleted child of the root node:
  ${eg{ db.treeChildrenOf(0, filterDeleted = Some(true)) === List() }}
  It is possible to delete the child node:
  ${eg{ db.treeDelete(child) === unit }}
  The deleted node can be accessed only as deleted node
  ${eg{ db.entry(1) === None }}
  ${eg{ db.treeEntryFor(1, filterDeleted = Some(true)) === Some(child) }}
  Now, the root node has no children:
  ${eg{ db.treeChildrenOf(0) === List() }}
  Accessing the deleted children of root finds the deleted entry:
  ${eg{ db.treeChildrenOf(0, filterDeleted = Some(true)) === List(child) }}
  Accessing historical views of the root children works as expected:
  ${eg{ db.treeChildrenOf(0, upToId = 0) === List() }}
  ${eg{ db.treeChildrenOf(0, upToId = 2) === List(child) }}
  ${eg{ db.treeChildrenOf(0, upToId = 3) === List() }}
  ${eg{ db.treeChildrenOf(0, upToId = 3, filterDeleted = Some(true)) === List(child) }}
  """

  val child = TreeEntry(1, 0, "child", None, None)
}
