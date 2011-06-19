// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.testutils

import java.io.File
import org.testng.annotations.{BeforeClass, AfterClass}

trait TestDataFileProvider {

  def testDataFile(name: String) : java.io.File =
    new java.io.File("testdata/" + getClass.getName.replace('.','/') + "/" + name)
  
}