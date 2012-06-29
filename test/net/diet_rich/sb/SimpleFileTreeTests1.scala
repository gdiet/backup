// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import net.diet_rich.util.Configuration.StringMap
import org.testng.annotations.Test
import org.testng.annotations.BeforeClass
import org.fest.assertions.Assertions.assertThat
import TestUtil._

class SimpleFileTreeTests1 {

  lazy val connection = DBConnection.hsqlMemoryDB()
  lazy val tree : TreeMethods = {
    TreeSqlDB dropTable connection
    TreeSqlDB createTable connection
    TreeSqlDB addInternalConstraints connection
    val dbSettings = Map("TreeDB.cacheSize"->"3")
    TreeDBCache(connection, dbSettings)
  }
    
  @Test
  def rootShouldBeEmptyStringWithId0 =
    assertThat(tree entry "" map (_ id)) isEqualTo Some(0L)
  
  @Test
  def createChildrenForInvalidIdShouldFail =
    assertThat(tree create (Long MaxValue, "invalid")) isEqualTo None

  @Test
  def createChildrenTwiceShouldFail = {
    val childName = randomString
    assertThat(tree create (0, childName) isDefined).isTrue
    assertThat(tree create (0, childName)) isEqualTo None
    assertThat(tree create ("/" + childName)) isEqualTo None
  }
    
  @Test
  def createPathWithMultipleElements = {
    assertThat(tree create "/"+randomString+"/some/more/subnodes" isDefined).isTrue
  }

  @Test
  def createPathWithSomeElementsMissing = {
    val baseName = randomString
    assertThat(tree create "/"+baseName+"/some/more/subnodes" isDefined).isTrue
    assertThat(tree create "/"+baseName+"/some/other/subnodes" isDefined).isTrue
  }

  @Test
  def getIdForPathWithSomeElements = {
    val baseName = randomString
    val childOpt = tree create "/"+baseName+"/some/subnodes"
    assertThat(tree entry "/"+baseName+"/some/subnodes" map (_ id)) isEqualTo childOpt isNotEqualTo None
    assertThat(tree entry "/"+baseName+"/some" isDefined).isTrue
  }

  @Test
  def getNameForPathWithSomeElements = {
    val path = "/"+randomString+"/some/subnodes"
    val childId = tree create path get;
    assertThat(tree entry childId map (_ name)) isEqualTo Some("subnodes")
  }

  @Test
  def nameForInvalidIdShouldFail =
    assertThat(tree entry Long.MaxValue) isEqualTo None

  @Test
  def getPathWithSomeElements = {
    val path = "/"+randomString+"/some/subnodes"
    val childId = tree create path get;
    assertThat(tree path childId) isEqualTo Some(path)
  }

  @Test
  def getEmptyChildrenListForLeaf = {
    val path = "/"+randomString+"/some/subnodes"
    val childId = tree create path get;
    assertThat(tree children childId isEmpty)
  }

  @Test
  def getChildrenListForNode = {
    val path = "/"+randomString
    val baseId = tree create path get;
    assertThat(tree create (baseId, "child1") isDefined).isTrue
    assertThat(tree create (baseId, "child2") isDefined).isTrue
    val children = tree children baseId map(_ name) toList;
    assertThat(children size) isEqualTo 2
    assertThat(children intersect List("child1","child2") size) isEqualTo 2
  }

  @Test
  def renamedNodesName = {
    val baseName = randomString
    val id = (tree create "/"+baseName+"/initialName").get
    val id2 = (tree create (id, "subName")).get
    assertThat(tree rename(id, "newName")).isTrue
    assertThat(tree.entry(id).get.name) isEqualTo "newName"
    assertThat(tree entry "/"+baseName+"/newName" map (_ id)) isEqualTo Some(id)
    assertThat(tree entry "/"+baseName+"/newName/subName" map (_ id)) isEqualTo Some(id2)
    assertThat(tree entry "/"+baseName+"/initialName") isEqualTo None
    assertThat(tree entry "/"+baseName+"/initialName/subName") isEqualTo None
    val base = (tree entry "/"+baseName).get.id
    assertThat(tree children base size) isEqualTo 1
    assertThat(tree.children(base).head.name) isEqualTo "newName"
    assertThat(tree.children(base).head.id) isEqualTo id
  }
  
  @Test
  def renameNegativeTests = {
    val baseName = randomString
    val id = (tree create "/"+baseName+"/initialName").get
    val id2 = (tree create "/"+baseName+"/anotherName").get
    assertThat(tree rename(id, "anotherName")).isFalse
    assertThat(tree rename(Long MaxValue, "anotherName")).isFalse
  }

  @Test
  def movedNode = {
    val baseName = randomString
    assertThat(tree create ("/"+baseName+"/A/B/C") isDefined).isTrue
    assertThat(tree create ("/"+baseName+"/X/X/C") isDefined).isTrue
    val parent = (tree entry "/"+baseName+"/A").get.id
    val child = (tree entry "/"+baseName+"/A/B").get.id
    val newParent = (tree entry "/"+baseName+"/X").get.id
    assertThat(tree move(child, newParent)).isTrue
    assertThat(tree entry "/"+baseName+"/X/B" map (_ id)) isEqualTo Some(child)
    assertThat(tree entry ("/"+baseName+"/X/B/C") isDefined).isTrue
    assertThat(tree entry ("/"+baseName+"/A/B") isDefined).isFalse
    assertThat(tree children parent size) isEqualTo 0
    assertThat(tree children newParent size) isEqualTo 2
  }

  @Test
  def moveNegativeTests = {
    val baseName = randomString
    assertThat(tree create ("/"+baseName+"/A/B") isDefined).isTrue
    val child = (tree entry "/"+baseName+"/A").get.id
    assertThat(tree move(child, Long MaxValue)).isFalse
    assertThat(tree move(Long MaxValue, child)).isFalse
    assertThat(tree move(Long MaxValue, Long MaxValue)).isFalse
  }

  @Test
  def deletedNode = {
    val baseName = randomString
    assertThat(tree create ("/"+baseName+"/A/B") isDefined).isTrue
    val parent = (tree entry "/"+baseName)
    val id = (tree entry "/"+baseName+"/A").get.id
    val child = (tree entry "/"+baseName+"/A/B").get.id
    assertThat(tree deleteWithChildren id).isTrue
    assertThat(tree entry "/"+baseName) isEqualTo parent
    assertThat(tree entry "/"+baseName+"/A") isEqualTo None
    assertThat(tree entry id) isEqualTo None
    assertThat(tree entry child) isEqualTo None
    assertThat(tree children parent.get.id size) isEqualTo 0
  }

  @Test
  def deleteNegativeTests = {
    assertThat(tree deleteWithChildren(Long MaxValue)).isFalse
  }

}
