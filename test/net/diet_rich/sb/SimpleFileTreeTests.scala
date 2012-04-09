// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import java.sql.Connection
import java.sql.DriverManager
import net.diet_rich.util.Configuration.StringMap
import org.testng.annotations.Test
import org.testng.annotations.BeforeClass
import org.fest.assertions.Assertions.assertThat
import TestUtil._

class SimpleFileTreeTests {

  lazy val tree = new Tree {
    val connection = DBConnection.hsqlMemoryDB
    TreeDB createTables connection
    TreeDB addInternalConstraints connection
    val dbSettings = Map("TreeDBCache.cacheSize"->"3")
    val treeDb: TreeDB = TreeDBCache(connection, dbSettings)
//    val treeDb: TreeDB = TreeDB(connection)
  }

  @Test
  def rootShouldBeEmptyStringWithId0 =
    assertThat(tree get "") isEqualTo Some(0L)
 
  @Test
  def createChildrenForInvalidIdShouldFail =
    assertThat(tree make (Long MaxValue, "invalid")) isEqualTo None

  @Test
  def createChildrenTwiceShouldFail = {
    val childName = randomString
    assertThat(tree make (0, childName) isDefined).isTrue
    assertThat(tree make (0, childName)) isEqualTo None
    assertThat(tree make ("/" + childName)) isEqualTo None
  }
    
  @Test
  def createPathWithMultipleElements = {
    assertThat(tree make "/"+randomString+"/some/more/subnodes" isDefined).isTrue
  }

  @Test
  def createPathWithSomeElementsMissing = {
    val baseName = randomString
    assertThat(tree make "/"+baseName+"/some/more/subnodes" isDefined).isTrue
    assertThat(tree make "/"+baseName+"/some/other/subnodes" isDefined).isTrue
  }

  @Test
  def getIdForPathWithSomeElements = {
    val baseName = randomString
    val childOpt = tree make "/"+baseName+"/some/subnodes"
    assertThat(tree get "/"+baseName+"/some/subnodes") isEqualTo childOpt isNotEqualTo None
    assertThat(tree get "/"+baseName+"/some" isDefined).isTrue
  }

  @Test
  def getNameForPathWithSomeElements = {
    val path = "/"+randomString+"/some/subnodes"
    val childId = tree make path get;
    assertThat(tree name childId) isEqualTo Some("subnodes")
  }

  @Test
  def nameForInvalidIdShouldFail =
    assertThat(tree name Long.MaxValue) isEqualTo None

  @Test
  def getPathWithSomeElements = {
    val path = "/"+randomString+"/some/subnodes"
    val childId = tree make path get;
    assertThat(tree path childId) isEqualTo Some(path)
  }

  @Test
  def getEmptyChildrenListForLeaf = {
    val path = "/"+randomString+"/some/subnodes"
    val childId = tree make path get;
    assertThat(tree children childId isEmpty)
  }

  @Test
  def getChildrenListForNode = {
    val path = "/"+randomString
    val baseId = tree make path get;
    assertThat(tree make (baseId, "child1") isDefined).isTrue
    assertThat(tree make (baseId, "child2") isDefined).isTrue
    val children = tree children baseId map(_ name) toList;
    assertThat(children size) isEqualTo 2
    assertThat(children intersect List("child1","child2") size) isEqualTo 2
  }

  @Test
  def renamedNodesName = {
    val baseName = randomString
    val id = (tree make "/"+baseName+"/initialName").get
    val id2 = (tree make (id, "subName")).get
    assertThat(tree rename(id, "newName")).isTrue
    assertThat(tree name id get) isEqualTo "newName"
    assertThat(tree get "/"+baseName+"/newName") isEqualTo Some(id)
    assertThat(tree get "/"+baseName+"/newName/subName") isEqualTo Some(id2)
    assertThat(tree get "/"+baseName+"/initialName") isEqualTo None
    assertThat(tree get "/"+baseName+"/initialName/subName") isEqualTo None
    val base = (tree get "/"+baseName).get
    assertThat(tree children base size) isEqualTo 1
    assertThat(tree.children(base).head.name) isEqualTo "newName"
    assertThat(tree.children(base).head.id) isEqualTo id
  }
  
  @Test
  def renameNegativeTests = {
    val baseName = randomString
    val id = (tree make "/"+baseName+"/initialName").get
    val id2 = (tree make "/"+baseName+"/anotherName").get
    assertThat(tree rename(id, "anotherName")).isFalse
    assertThat(tree rename(Long MaxValue, "anotherName")).isFalse
  }

  @Test
  def movedNode = {
    val baseName = randomString
    assertThat(tree make ("/"+baseName+"/A/B/C") isDefined).isTrue
    assertThat(tree make ("/"+baseName+"/X/X/C") isDefined).isTrue
    val parent = (tree get "/"+baseName+"/A").get
    val child = (tree get "/"+baseName+"/A/B").get
    val newParent = (tree get "/"+baseName+"/X").get
    assertThat(tree move(child, newParent)).isTrue
    assertThat(tree get "/"+baseName+"/X/B") isEqualTo Some(child)
    assertThat(tree get ("/"+baseName+"/X/B/C") isDefined).isTrue
    assertThat(tree get ("/"+baseName+"/A/B") isDefined).isFalse
    assertThat(tree children parent size) isEqualTo 0
    assertThat(tree children newParent size) isEqualTo 2
  }

  @Test
  def moveNegativeTests = {
    val baseName = randomString
    assertThat(tree make ("/"+baseName+"/A/B") isDefined).isTrue
    val child = (tree get "/"+baseName+"/A").get
    assertThat(tree move(child, Long MaxValue)).isFalse
    assertThat(tree move(Long MaxValue, child)).isFalse
    assertThat(tree move(Long MaxValue, Long MaxValue)).isFalse
  }

  @Test
  def deletedNode = {
    val baseName = randomString
    assertThat(tree make ("/"+baseName+"/A/B") isDefined).isTrue
    val parent = (tree get "/"+baseName).get
    val id = (tree get "/"+baseName+"/A").get
    val child = (tree get "/"+baseName+"/A/B").get
    assertThat(tree deleteWithChildren id).isTrue
    assertThat(tree get "/"+baseName) isEqualTo Some(parent)
    assertThat(tree get "/"+baseName+"/A") isEqualTo None
    assertThat(tree name id) isEqualTo None
    assertThat(tree name child) isEqualTo None
    assertThat(tree children parent size) isEqualTo 0
  }

  @Test
  def deleteNegativeTests = {
    assertThat(tree deleteWithChildren(Long MaxValue)).isFalse
  }

}