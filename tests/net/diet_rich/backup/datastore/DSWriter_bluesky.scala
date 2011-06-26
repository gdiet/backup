// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.datastore

import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.Test
import org.mockito.Mockito._
import net.diet_rich.testutils._
import net.diet_rich.util.io.Streams._

class DSWriter_bluesky extends TempFileProvider {

  @Test
  def blueSky_DataStoreWriter_ECCalculation : Unit = {
    val settings = new DSSettings { override val dataFileChunkSize = 10 }
    val ecFile = newTempFile
    val dfFile = newTempFile

    // set up test data file
    usingIt(new java.io.RandomAccessFile(ecFile, "rw")) { raf =>
      val initialData = Array[Byte](-1, -1, -1, -1, -1,    -1, -1, -1, 0, 0)
      val checksum = settings.newDataFileChecksum
      checksum.update(initialData, 0, initialData.length)
      raf write initialData
      raf writeLong checksum.getValue
    }

    // override and add methods for testing
    trait TestAdapter {
      protected val dataArray : Array[Byte]
      var timesCloseCalled = 0
      def getData = dataArray
    }
    
    val ecWriter = new DSWECFile(settings, ecFile, Option(ecFile)) with TestAdapter {
      override def close = { timesCloseCalled = timesCloseCalled + 1; super.close }
    }

    val dfWriter = new DSWDataFile(settings, 1L, dfFile, ecWriter, None) with TestAdapter {
      override def close = { timesCloseCalled = timesCloseCalled + 1; super.close }
      def publicViews = views
    }

    // create views
    val view1 = dfWriter.makeView(2, 5)
    val view2 = dfWriter.makeView(7, 10)
    assertThat(dfWriter.publicViews contains view1).isTrue
    assertThat(dfWriter.publicViews contains view2).isTrue
    
    def assertStoreResult(result: DSWDataFileResult)(position: Int, length: Int, fileID: Long)= {
      assertThat(result.position) isEqualTo position
      assertThat(result.length) isEqualTo length
      assertThat(result.fileID) isEqualTo fileID
    }
    
    // store data
    assertStoreResult (view1.store(Array(1,2,3,4), 0, 4))   (2, 3, 1L) // fill view1 up to max
    assertThat(dfWriter.publicViews contains view1).isFalse // view1 now must be unregistered
    assertThat(dfWriter.timesCloseCalled).isZero // but data file writer must not have been closed
    assertStoreResult (view1.store(Array(1,2,3,4), 0, 4))   (5, 0, 1L) // view1 is full - may not take more
    
    assertStoreResult (view2.store(Array(1,2,3,4), 2, 2))   (7, 2, 1L) // fill view2 partially
    assertStoreResult (view2.store(Array(1,2,3,4), 0, 4))   (9, 1, 1L) // fill view2 up to max
    assertThat(dfWriter.publicViews contains view2).isFalse // view2 now must be unregistered
    assertThat(dfWriter.timesCloseCalled) isEqualTo 1 // data file writer now must be closed
    assertStoreResult (view2.store(Array(1,2,3,4), 0, 4))   (10, 0, 1L) // view2 is full - may not take more

    // close EC writer
    assertThat(ecWriter.timesCloseCalled).isZero
    ecWriter.close
    assertThat(ecWriter.timesCloseCalled) isEqualTo 1

    def checkData(writer: {def getData: Array[Byte]}, data: Array[Byte], file: java.io.File) = {
      // check data in memory
      assertThat(writer.getData) isEqualTo data
      // check data in data file
      val raf = closeLater(new java.io.RandomAccessFile(file, "r"))
      val read = new Array[Byte](settings.dataFileChunkSize)
      raf.readFully(read)
      assertThat(read) isEqualTo data
      // check checksum in data file
      val check = settings.newDataFileChecksum
      check.update(read, 0, read.length)
      assertThat(check.getValue) isNotEqualTo 0
      assertThat(check.getValue) isEqualTo raf.readLong
    }

    checkData(dfWriter, Array[Byte](0, 0, 1, 2, 3,    0, 0, 3, 4, 1), dfFile)
    checkData(ecWriter, Array[Byte](-1, -1, -2, -3, -4,    -1, -1, -4, 4, 1), ecFile)
  }

}