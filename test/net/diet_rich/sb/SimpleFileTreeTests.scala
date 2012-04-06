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

  val tree = new Tree {
    val connection = DBConnection.hsqlMemoryDB
    TreeDB createTables connection
    TreeDB addInternalConstraints connection
    val dbSettings = Map("TreeDBCache.cacheSize"->"3")
    val treeDb: TreeDB = TreeDBCache(connection, dbSettings)
  }
  
  @Test
  def rootShouldBeEmptyStringWithId0 =
    assertThat(tree get "") isEqualTo Some(0L)
 
  @Test
  def createChildrenForInvalidIdShouldFail =
    assertThat(tree make (Long.MaxValue, "invalid")) isEqualTo None

  @Test
  def createChildrenTwiceShouldFail = {
    val childName = randomString
    assertThat(tree make (0, childName) isDefined)
    assertThat(tree make (0, childName)) isEqualTo None
  }
    
  @Test
  def createPathWithMultipleElements = {
    assertThat(tree make "/"+randomString+"/some/more/subnodes" isDefined)
  }

  @Test
  def createPathWithSomeElementsMissing = {
    val baseName = randomString
    assertThat(tree make "/"+baseName+"/some/more/subnodes" isDefined)
    assertThat(tree make "/"+baseName+"/some/other/subnodes" isDefined)
  }

  @Test
  def getIdForPathWithSomeElements = {
    val baseName = randomString
    val childOpt = tree make "/"+baseName+"/some/subnodes"
    assertThat(tree get "/"+baseName+"/some/subnodes") isEqualTo childOpt isNotEqualTo None
    assertThat(tree get "/"+baseName+"/some" isDefined)
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
    assertThat(tree make (baseId, "child1") isDefined)
    assertThat(tree make (baseId, "child2") isDefined)
    val children = tree children baseId map(_ name) toList;
    assertThat(children size) isEqualTo 2
    assertThat(children intersect List("child1","child2") size) isEqualTo 2
  }
  
}