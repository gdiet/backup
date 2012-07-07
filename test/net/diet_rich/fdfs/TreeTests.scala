// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.fdfs

import org.testng.annotations.{BeforeMethod,Test}
import org.fest.assertions.Assertions.assertThat
import net.diet_rich.sb.TestUtil._

class TreeTests {

  lazy val connection = DBConnection.hsqlMemoryDB()
  lazy val treedb: TreeDB = new TreeSqlDB()(connection)
  
  @BeforeMethod
  def setupTest: Unit = {
    TreeSqlDB dropTable connection
    TreeSqlDB createTable connection
  }
  
  @Test
  def rootShouldBeEmptyStringWithId0 = {
    assertThat(treedb.entry(TreeDB.ROOTID).get.name) isEqualTo ""
    assertThat(treedb.entry(TreeDB.ROOTID).get.id) isEqualTo 0
    assertThat(treedb.entry(TreeDB.ROOTID).get.parent) isEqualTo -1
  }

  protected def expectChildren(id: Long, children: List[String]) = {
    val actualChildren = treedb children id map(_ name) toList;
    assertThat(actualChildren size) isEqualTo children.size
    assertThat(actualChildren intersect children size) isEqualTo children.size
  }
  
  @Test
  def createChildrenForInvalidIdIsAllowed = {
    treedb create (Long MaxValue, "childName")
    expectChildren(Long.MaxValue, List("childName"))
  }

  @Test
  def createDuplicatesIsAllowed = {
    treedb create (0, "childName")
    treedb create (0, "childName")
    expectChildren(0, List("childName", "childName"))
  }

  @Test
  def noEntryReturnedForIdNotStored =
    assertThat(treedb entry Long.MaxValue) isEqualTo None

  @Test
  def leafHasEmptyChildrenList = {
    val childid = treedb create (TreeDB.ROOTID, "childname")
    expectChildren(childid, List())
  }

  @Test
  def childrenListForNodeIsOK = {
    val childid0 = treedb create (TreeDB.ROOTID, "childname")
    val childid1 = treedb create (childid0, "child1")
    val childid2 = treedb create (childid0, "child2")
    expectChildren(TreeDB.ROOTID, List("childname"))
    expectChildren(childid0, List("child1","child2"))
  }

  @Test
  def renamedNodesNameIsOK = {
    val id = treedb create (TreeDB.ROOTID, "baseName")
    val id2 = treedb create (id, "subName")
    treedb rename(id2, "newName")
    assertThat(treedb.entry(id2).get.name) isEqualTo "newName"
    expectChildren(id, List("newName"))
  }
  
  @Test
  def renameMayCreateDuplicates = {
    val id = treedb create (TreeDB.ROOTID, "baseName")
    val id2 = treedb create (id, "subName")
    val id3 = treedb create (id, "subName2")
    treedb rename(id2, "subName2")
    expectChildren(id, List("subName2", "subName2"))
  }

  @Test
  def renamingUnknownNodesIsPossible =
    treedb rename(Long MaxValue, "anotherName")
  
  @Test
  def movedNodesDisappearAndReappearInADifferenPosition = {
    val id = treedb create (TreeDB.ROOTID, "baseName")
    val id2 = treedb create (id, "subName")
    val id3 = treedb create (id, "subName2")
    treedb move(id3, id2)
    expectChildren(id, List("subName"))
    expectChildren(id2, List("subName2"))
    expectChildren(id3, List())
  }

  @Test
  def movingUnknownNodesIsPossible =
    treedb move(Long MaxValue, 0)

  @Test
  def deletedNodesDisappear = {
    val id = treedb create (TreeDB.ROOTID, "baseName")
    val id2 = treedb create (id, "subName")
    val id3 = treedb create (id2, "subName2")
    val id4 = treedb create (id, "subName2")
    treedb deleteWithChildren id2
    expectChildren(id, List("subName2"))
    assertThat(treedb entry id2 isEmpty) isTrue;
    assertThat(treedb entry id3 isEmpty) isTrue
  }

  @Test
  def deletingUnknownNodesIsPossible =
    treedb deleteWithChildren(Long MaxValue)
}