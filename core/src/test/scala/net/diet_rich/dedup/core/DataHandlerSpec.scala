// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import java.io.ByteArrayInputStream

import org.specs2.SpecificationWithJUnit

import net.diet_rich.dedup.core.values.{Hash, Print, Size, StoreMethod}

class DataHandlerSpec extends SpecificationWithJUnit { def is = s2"""
${"Tests for the data handling part".title}

Storing zero bytes should be possible $storeZeroBytes
  """

  def storeZeroBytes = {
    val dataHandler = new sql.InMemoryDBPartWithTables
      with InMemoryDataBackendPart
      with DataHandlerPart
      with sql.TablesPart
      with FreeRangesPart
      with StoreSettingsSlice {
      // TODO both store methods
      def storeSettings = StoreSettings ("MD5", 4, StoreMethod.DEFLATE)
    }

    // TODO check against StoreRestoreSpec
    dataHandler.inLifeCycle {
      val source = Source.fromInputStream(new ByteArrayInputStream(Array()), Size(0))
      dataHandler.dataHandler.storeSourceData(source)
      // TODO also test this
//      dataHandler.dataHandler.storePackedData(Iterator(), Size(0), Print(0), Hash(Array()))
      success
    }
  }
}
