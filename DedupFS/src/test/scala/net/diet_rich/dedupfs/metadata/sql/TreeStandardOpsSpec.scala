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
  ${eg{ db.children(0) === List() }}
  The first child to be inserted gets the key 1:
  ${eg{ db.create(0, "child", None, None) === 1 }}
  The second child, inserted as child of the first child, gets the key 2:
  ${eg{ db.create(1, "grandChild", None, None) === 2 }}
  The root node now has exactly one child:
  ${eg{ db.children(0) === List(child) }}
  It is possible to delete the child node:
  ${eg{ db.treeDelete(child) === unit }}
  The deleted node is not found by the standard methods
  ${eg{ db.entry(1) === None }}
  Now, the root node has no children:
  ${eg{ db.children(0) === List() }}
  """

  val child = TreeEntry(1, 0, "child", None, None)
}
