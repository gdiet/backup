package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values.{TreeEntryID, TreeEntry}
import org.specs2.SpecificationWithJUnit

class CheckRepositorySpec extends SpecificationWithJUnit { def is = s2"""
The method to detect cyclic parent relations in the tree entries should
  $onlyTreeRoot
  $regularTree
  $irregularTree
  $smallLoop
"""
  import CheckRepositoryApp.cyclicParentRelations
  val root = FileSystem.ROOTENTRY
  def t(id: Int, parent: Int) = TreeEntry(TreeEntryID(id), TreeEntryID(parent), "", None, None, None)
  def detectCycle(treeEntries: List[TreeEntry]) = cyclicParentRelations(treeEntries sortBy (_.id.value) iterator)

  def onlyTreeRoot = detectCycle(List(root)) should beEmpty
  def regularTree = detectCycle(List(root, t(1,0), t(2,1), t(3,0), t(4,5), t(5,2), t(7,9), t(9,3))) should beEmpty
  def irregularTree = detectCycle(List(root, t(1,0), t(2,1), t(3,7), t(4,5), t(5,2), t(7,9), t(9,3))) should beEqualTo(List(t(9,3), t(7,9), t(3,7)))
  def smallLoop = detectCycle(List(root, t(1,1))) should beEqualTo(List(t(1,1)))
}
