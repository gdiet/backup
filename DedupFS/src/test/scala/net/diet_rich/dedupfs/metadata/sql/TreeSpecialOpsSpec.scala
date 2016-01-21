package net.diet_rich.dedupfs.metadata.sql

import java.io.IOException

import org.specs2.Specification
import org.specs2.specification.AfterAll

import net.diet_rich.common.test._
import net.diet_rich.dedupfs.metadata.TreeEntry

class TreeSpecialOpsSpec extends Specification with AfterAll with TestsHelper with PackageHelper {
  def is = sequential ^ s2"""
Advanced tests for the tree database, starting with a newly initialized database:
  Initially, the root node has no children:
  ${eg{ db.treeChildrenOf(0) === List() }}
  The first child to be inserted gets the key 1:
  ${eg{ db.create(0, "child", None, None) === 1 }}
  Inserting a child with the same name is not possible if using the standard create method:
  ${eg{ db.create(0, "child", None, None) should throwA[IOException] }}
  Inserting a child with the same name is possible if using the unchecked create method:
  ${eg{ db.createUnchecked(0, "child", None, None) === 2 }}
  Moving a node should be possible:
  ${eg{ db.change(TreeEntry(1, 2, "child", None, None)) === true }}
  The root node now has exactly one child:
  ${eg{ db.children(0) === List(TreeEntry(2, 0, "child", None, None)) }}
  The second child now has exactly one child:
  ${eg{ db.children(2) === List(TreeEntry(1, 2, "child", None, None)) }}
  """
}
