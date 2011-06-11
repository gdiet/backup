// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.datastore

import org.testng.annotations.Test
import org.testng.Assert._
import org.mockito.Mockito._

class DSWriter_bluesky {

  @Test
  def blueSky_inMemory_DataStoreWriter_ECCalculation {
    val dummyFile = new java.io.File("testfiles/dummy")
    val settings = new DSSettings { override val dataFileChunkSize = 10 }
    
    // override and add methods for testing
    val ecWriter = new DSWECFile(settings, dummyFile, None) {
      var timesCloseCalled = 0
      override def close = { timesCloseCalled = timesCloseCalled + 1 }
      def setData(bytes: Array[Byte]) = { for (n <- 0 until bytes.length) dataArray(n) = bytes(n) }
      def getData = dataArray
    }

    val dfWriter = new DSWDataFile(settings, 1L, dummyFile, ecWriter, None) {
      var timesCloseCalled = 0
      override def close = { timesCloseCalled = timesCloseCalled + 1 }
      def viewIsListed(view: DSWDataFileView) = views contains view
      def getData = dataArray
    }

    // set up test data
    ecWriter.setData(Array(-1, -1, -1, -1, -1,    -1, -1, -1))
    
    // create views
    val view1 = dfWriter.makeView(2, 5)
    val view2 = dfWriter.makeView(7, 10)
    
    // store data
    {
      val store1a = view1.store(Array(1,2,3,4), 0, 4) // fill view1 up to max
      assertEquals(store1a.position, 2)
      assertEquals(store1a.length, 3) 
      assertEquals(store1a.fileID, 1L)
    }
    {
      val store1b = view1.store(Array(1,2,3,4), 0, 4) // view1 is full - may not take more
      assertEquals(store1b.length, 0)
    }
    assertFalse(dfWriter viewIsListed view1) // view1 must already be unregistered
    assertTrue(dfWriter viewIsListed view2)
    assertEquals(dfWriter timesCloseCalled, 0);
    {
      val store2a = view2.store(Array(1,2,3,4), 2, 2) // fill view2 partially
      assertEquals(store2a.position, 7)
      assertEquals(store2a.length, 2) 
      assertEquals(store2a.fileID, 1L)
    }
    {
      val store2b = view2.store(Array(1,2,3,4), 0, 4) // fill view2 up to max
      assertEquals(store2b.position, 9)
      assertEquals(store2b.length, 1) 
      assertEquals(store2b.fileID, 1L)
    }
    assertFalse(dfWriter viewIsListed view2) // view2 must already be unregistered
    assertEquals(dfWriter timesCloseCalled, 1); // data file writer must already be closed
    {
      val store2c = view2.store(Array(1,2,3,4), 0, 4) // view2 is full - may not take more
      assertEquals(store2c.length, 0)
    }
    // check data
    assertEquals(ecWriter timesCloseCalled, 0) // data file writer must already be closed
    assertEquals(dfWriter.getData, Array[Byte](0, 0, 1, 2, 3,    0, 0, 3, 4, 1))
    assertEquals(ecWriter.getData, Array[Byte](-1, -1, -2, -3, -4,    -1, -1, -4, 4, 1))
  }
  
}