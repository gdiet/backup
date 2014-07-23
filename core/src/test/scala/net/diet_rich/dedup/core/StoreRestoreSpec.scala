// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import org.specs2.SpecificationWithJUnit

import net.diet_rich.dedup.core.values.{Bytes, Position, DataRange, Path, StoreMethod, Time}
import net.diet_rich.dedup.util.init

import FileSystem.{BasicPart, ROOTID}

class StoreRestoreSpec extends SpecificationWithJUnit { def is = s2"""
${"Tests for storing and restoring data".title}

Simple store and subsequent restore should be possible $storeRestore
  """

  def storeRestore = {
    val fs: FileSystem = new FileSystem
      with sql.InMemoryDBPartWithTables
      with InMemoryDataBackendPart
      with StoreLogic
      with Tree
      with DataHandlerPart
      with sql.TablesPart
      with FreeRangesPart
      with StoreSettingsSlice {
      def storeSettings = StoreSettings ("MD5", 4, StoreMethod.DEFLATE)
      // TODO tests for STORE and DEFLATE
    }

    fs.inLifeCycle {
      val sourceData = init(Bytes.zero(1000)){ b => new util.Random(43) nextBytes b.data }
      val source = new InMemorySource(sourceData)
      fs storeUnchecked (ROOTID, "child", source, Time(0))
      val dataid = fs.entries(Path("/child")).head.data.get
      val data = fs.read(dataid)
      val flat = data.flatMap(b => b.data.drop(b.offset).take(b.length)).toList
      flat === sourceData.data.toList
    }
  }
}
