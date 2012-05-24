// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import net.diet_rich.util.Configuration.StringMap
import org.testng.annotations.Test
import org.testng.annotations.BeforeClass
import org.fest.assertions.Assertions.assertThat
import TestUtil._

class SimpleFileTreeTests2 {

  lazy val treedb: TreeDB = {
    val connection = DBConnection.hsqlMemoryDB()
    TreeSqlDB dropTable connection
    TreeSqlDB createTable connection
    TreeSqlDB addInternalConstraints connection
    val dbSettings = Map("TreeDB.cacheSize"->"3")
    TreeDBCache(connection, dbSettings)
//    TreeSqlDB(connection)
  }
  
  @Test
  def rootShouldBeEmptyStringWithId0 = {
    assertThat(treedb.entry(TreeDB.ROOTID).get.name) isEqualTo ""
    assertThat(treedb.entry(TreeDB.ROOTID).get.id) isEqualTo 0
    assertThat(treedb.entry(TreeDB.ROOTID).get.parent) isEqualTo 0
  }
 
  @Test
  def createChildrenForInvalidIdShouldFail =
    assertThat(treedb create (Long MaxValue, "invalid")) isEqualTo None

  @Test
  def createChildrenTwiceShouldFail = {
    val childName = randomString
    assertThat(treedb create (0, childName) isDefined).isTrue
    assertThat(treedb create (0, childName)) isEqualTo None
  }

  @Test
  def entryForInvalidIdShouldFail =
    assertThat(treedb entry Long.MaxValue) isEqualTo None

  @Test
  def getEmptyChildrenListForLeaf = {
    val childname = randomString
    val childid = treedb create (TreeDB.ROOTID, childname) get;
    assertThat(treedb children childid isEmpty)
  }

  @Test
  def getChildrenListForNode = {
    val childname = randomString
    val childid0 = treedb create (TreeDB.ROOTID, childname) get;
    val childid1 = treedb create (childid0, "child1") get;
    val childid2 = treedb create (childid0, "child2") get;
    val children = treedb children childid0 map(_ name) toList;
    assertThat(children size) isEqualTo 2
    assertThat(children intersect List("child1","child2") size) isEqualTo 2
  }

  @Test
  def renamedNodesName = {
    val baseName = randomString
    val id = treedb create (TreeDB.ROOTID, baseName) get;
    val id2 = treedb create (id, "subName") get;
    assertThat(treedb rename(id2, "newName")).isTrue
    assertThat(treedb.entry(id2).get.name) isEqualTo "newName"
    val children = treedb children id map(_ name) toList;
    assertThat(children size) isEqualTo 1
    assertThat(children head) isEqualTo "newName"
  }
  
  @Test
  def renameNegativeTests = {
    val baseName = randomString
    val id = treedb create (TreeDB.ROOTID, baseName) get;
    val id2 = treedb create (id, "subName") get;
    val id3 = treedb create (id, "subName2") get;
    assertThat(treedb rename(id2, "subName2")).isFalse
    assertThat(treedb rename(Long MaxValue, "anotherName")).isFalse
  }

  @Test
  def movedNode = {
    val baseName = randomString
    val id = treedb create (TreeDB.ROOTID, baseName) get;
    val id2 = treedb create (id, "subName") get;
    val id3 = treedb create (id, "subName2") get;
    assertThat(treedb move(id3, id2)).isTrue
    val children1 = treedb children id map(_ name) toList;
    assertThat(children1 size) isEqualTo 1
    assertThat(children1 head) isEqualTo "subName"
    val children2 = treedb children id2 map(_ name) toList;
    assertThat(children2 size) isEqualTo 1
    assertThat(children2 head) isEqualTo "subName2"
  }

  @Test
  def moveNegativeTests = {
    val baseName = randomString
    val id = treedb create (TreeDB.ROOTID, baseName) get;
    val id2 = treedb create (id, "subName") get;
    val id3 = treedb create (id2, "subName2") get;
    val id4 = treedb create (id, "subName2") get;
    assertThat(treedb move(id4, id2)).isFalse
    assertThat(treedb move(id3, id)).isFalse
    assertThat(treedb move(Long MaxValue, id)).isFalse
    assertThat(treedb move(id3, Long MaxValue)).isFalse
  }

  @Test
  def deletedNode = {
    val baseName = randomString
    val id = treedb create (TreeDB.ROOTID, baseName) get;
    val id2 = treedb create (id, "subName") get;
    val id3 = treedb create (id2, "subName2") get;
    val id4 = treedb create (id, "subName2") get;
    assertThat(treedb deleteWithChildren id2).isTrue
    val children = treedb children id map(_ name) toList;
    assertThat(children size) isEqualTo 1
    assertThat(children head) isEqualTo "subName2"
  }

  @Test
  def deleteNegativeTests = {
    assertThat(treedb deleteWithChildren(Long MaxValue)).isFalse
  }

}