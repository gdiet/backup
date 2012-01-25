// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich

import org.testng.annotations.Test
import org.testng.annotations.AfterClass

class ScalaAndTestNG {

  private var testMethodWithReturnTypeHasRun = false
  
  @Test
  def testMethodWithReturnType : Int = {
    testMethodWithReturnTypeHasRun = true
    1
  }

  // needed to enforce that the @AfterClass method is run
  @Test
  def testMethodWithoutReturnType : Unit = Unit
  
  @AfterClass
  def checkThatTestMethodWithReturnTypeHasRun : Unit =
    assert (testMethodWithReturnTypeHasRun)
  
}