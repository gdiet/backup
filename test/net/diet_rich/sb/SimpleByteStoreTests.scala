// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import org.testng.annotations.Test
import org.fest.assertions.Assertions.assertThat
import TestUtil._

class SimpleByteStoreTests {

  lazy val connection = DBConnection.hsqlMemoryDB()
  lazy val store = {
    val repoSettings = Map("hash algorithm" -> "MD5")
    ByteStoreSqlDB.createTable(connection, repoSettings)
    ByteStoreSqlDB.addInternalConstraints(connection)
    ByteStoreSqlDB(connection)
  }
  
  @Test
  def readAMissingEntry = assertThat(store.read(-2) isEmpty) isTrue
  
  @Test
  def storeAndReadASimpleEntry = {
    var count = 0
    var start = 0L
    store.write(1){ range =>
      count = count + 1
      if (count == 1) start = range.start
      assertThat(range length) isGreaterThan 10
      if (count == 1) range reduce 10 else range reduce 0
    }
    assertThat(count) isEqualTo 2
    val read = store.read(1)
    assertThat(read size) isEqualTo 1
    assertThat(read.head length) isEqualTo 10
    assertThat(read.head start) isEqualTo start
  }

  @Test
  def storeAndReadTwoBlocks = {
    var count = 0
    var range1 = DataRange(0,0)
    var start2 = 0L
    store.write(2){ range =>
      assertThat(range length) isGreaterThan 10
      count = count + 1
      if (count == 1) {
        range1 = range reduce 10
        range1
      } else {
        if (count == 2) start2 = range.start
        if (count == 2) range reduce 10 else range reduce 0
      }
    }
    assertThat(count) isEqualTo 3
    val read = store.read(2)
    assertThat(read size) isEqualTo 2
    assertThat(read.head) isEqualTo range1
    assertThat(read.tail.head start) isEqualTo start2
    assertThat(read.tail.head length) isEqualTo 10
  }

  @Test
  def storeAndReadOneFullBlock = {
    var count = 0
    var range1 = DataRange(0,0)
    store.write(3){ range =>
      assertThat(range length) isGreaterThan 0
      count = count + 1
      if (count == 1) {
        range1 = range
        range
      } else {
        range reduce 0
      }
    }
    assertThat(count) isEqualTo 2
    val read = store.read(3)
    assertThat(read size) isEqualTo 1
    assertThat(read.head) isEqualTo range1
  }
  
}