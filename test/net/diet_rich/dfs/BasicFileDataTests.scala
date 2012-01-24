// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.Test
import DataDefinitions.TimeAndData
import TestFS.{memfs => fs}
import TestUtil._

class BasicFileDataTests {

  @Test
  def storeZeroByteFile = {
    val id = fs make "/"+randomString get;
    fs store(id, TimeAndData(1234,0))
    assertThat(fs time id get) isEqualTo 1234
    assertThat(fs size id get) isEqualTo 0
  }
  
}