// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import org.specs2.SpecificationWithJUnit

import net.diet_rich.dedup.core.FileSystem.ROOTID
import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.testutil.newTestDir
import net.diet_rich.dedup.util.init

class SystemTestSpec extends SpecificationWithJUnit { def is = s2"""
${"System test using the external interface".title}

Store and subsequent restore should be possible $storeRestore
  """

  def storeRestore = {
    val repository = init(newTestDir("SystemTestSpec")){Repository create (_, dataBlockSize = Size(120))}
    Repository(repository, storeMethod = StoreMethod.STORE){ fs =>
      val sourceData = init(Bytes.zero(1000)){ b => new util.Random(43) nextBytes b.data }
      val source = new InMemorySource(sourceData)
      fs storeUnchecked (ROOTID, "child", source, Time(0))
      val dataid = fs.entries(Path("/child")).head.data.get
      val data = fs read dataid
      val flat = data.flatMap(b => b.data.drop(b.offset).take(b.length)).toList
      val flatSource = sourceData.data.toList
      (flat.size === flatSource.size) and
        ((flat == flatSource) === true)
    }
  }
}
