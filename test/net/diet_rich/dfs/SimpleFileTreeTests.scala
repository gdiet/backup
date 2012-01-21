// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.Test
import TestFS.{memfs => fs}
import TestUtil._

class SimpleFileTreeTests {

  @Test
  def rootShouldBeEmptyStringWithId0 =
    assertThat(fs get "") isEqualTo Some(0L)
 
  @Test
  def createChildrenForInvalidIdShouldFail =
    expectThat(fs make (Long.MaxValue, "invalid")) doesThrow new Exception

  @Test
  def createChildrenTwiceShouldFail = {
    val childName = randomString
    assertThat(fs make (0, childName) isDefined)
    assertThat(fs make (0, childName) isEmpty)
  }
    
  @Test
  def createPathWithMultipleElements = {
    assertThat(fs make "/"+randomString+"/some/more/subnodes" isDefined)
  }

  @Test
  def createPathWithSomeElementsMissing = {
    val baseName = randomString
    assertThat(fs make "/"+baseName+"/some/more/subnodes" isDefined)
    assertThat(fs make "/"+baseName+"/some/other/subnodes" isDefined)
  }

  @Test
  def getIdForPathWithSomeElements = {
    val baseName = randomString
    val childOpt = fs make "/"+baseName+"/some/subnodes"
    assertThat(fs get "/"+baseName+"/some/subnodes") isEqualTo childOpt isNotEqualTo None
    assertThat(fs get "/"+baseName+"/some" isDefined)
  }

  @Test
  def getNameForPathWithSomeElements = {
    val path = "/"+randomString+"/some/subnodes"
    val childId = fs make path get;
    assertThat(fs name childId) isEqualTo Some("subnodes")
  }

  @Test
  def nameForInvalidIdShouldFail =
    assertThat(fs name Long.MaxValue) isEqualTo None

  @Test
  def getPathWithSomeElements = {
    val path = "/"+randomString+"/some/subnodes"
    val childId = fs make path get;
    assertThat(fs path childId) isEqualTo Some(path)
  }

  @Test
  def getEmptyChildrenListForLeaf = {
    val path = "/"+randomString+"/some/subnodes"
    val childId = fs make path get;
    assertThat(fs children childId isEmpty)
  }

  @Test
  def getChildrenListForNode = {
    val path = "/"+randomString
    val baseId = fs make path get;
    assertThat(fs make (baseId, "child1") isDefined)
    assertThat(fs make (baseId, "child2") isDefined)
    assertThat(fs children baseId intersect List("child1","child2") isEmpty)
  }
  
}