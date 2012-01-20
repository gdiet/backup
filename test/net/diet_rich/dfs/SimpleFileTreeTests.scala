// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.Test
import TestFS.{memfs => fs}

class SimpleFileTreeTests {

  @Test
  def rootShouldBeEmptyStringWithId0 =
    assertThat(fs get "") isEqualTo(Some(0L))
  
}