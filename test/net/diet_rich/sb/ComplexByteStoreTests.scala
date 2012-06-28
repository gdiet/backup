// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import org.testng.annotations.Test
import org.fest.assertions.Assertions.assertThat
import TestUtil._
import org.testng.annotations.BeforeTest

class ComplexByteStoreTests {

//  lazy val connection = DBConnection.hsqlMemoryDB("ComplexByteStoreTests")

  @BeforeTest
  def setup {
//    val repoSettings = Map("hash algorithm" -> "MD5")
//    ByteStoreSqlDB.createTable(connection, repoSettings)
//    ByteStoreSqlDB.addInternalConstraints(connection)
  }
  
  @Test
  def freeGapsShouldBeFoundInitially = {
    val connection = DBConnection.hsqlMemoryDB("ComplexByteStoreTests")
    
    val repoSettings = Map("hash algorithm" -> "MD5")
    ByteStoreSqlDB.createTable(connection, repoSettings)
    ByteStoreSqlDB.addInternalConstraints(connection)

    val store1 = ByteStoreSqlDB(connection)
    var count = 0
    var range1Start = 0L;
    store1.write(1){ range =>
      count = count + 1
      if (count == 1) {
        var count2 = 0
        store1.write(2){ range =>
          count2 = count2 + 1
          if (count2 == 1) 10 else 0
        }
        range1Start = range.start
        10
      } else 0
    }
    
    val store2 = ByteStoreSqlDB(connection)
    var count3 = 0
    store2.write(3){ range =>
      assertThat(range.start).isEqualTo(range1Start + 10)
      0
    }
  }
  
}