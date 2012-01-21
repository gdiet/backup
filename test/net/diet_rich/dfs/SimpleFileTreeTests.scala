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
    assertThat(fs get "") isEqualTo (Some(0L))
 
  @Test
  def createChildrenForInvalidIdShouldFail =
    expectThat(fs make (Long.MaxValue, "invalid")) doesThrow new Exception

  @Test
  def createChildrenTwiceShouldFail = {
    val childname = randomString
    assertThat(fs make (0, childname) isDefined)
    assertThat(fs make (0, childname) isEmpty)
  }
    
}