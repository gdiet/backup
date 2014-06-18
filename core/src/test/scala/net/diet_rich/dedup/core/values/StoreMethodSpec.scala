// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

import org.specs2.SpecificationWithJUnit

import net.diet_rich.dedup.util.init

class StoreMethodSpec  extends SpecificationWithJUnit { def is = s2"""
${"Tests for store compression/decompresion".title}

Compression should be possible $compress
Compression and decompression should return the original data $compressDecompress
  """

  val randomData = init(Bytes.zero(1000)){ b => new util.Random(42) nextBytes b.data }
  val zeroData = Bytes.zero(5000)
  lazy val packed = StoreMethod.DEFLATE.pack(Iterator(randomData, zeroData)).toList
  lazy val unpacked = StoreMethod.DEFLATE.unpack(packed.iterator).toList

  def compress =
    (packed should haveSize(1)) and
      (packed.map(_.length) should contain(beBetween(1000, 1100)).forall)

  def compressDecompress =
    (unpacked should haveSize(1)) and
      (unpacked.head.data.toList.take(1000) === randomData.data.toList) and
      (unpacked.head.length === 6000) and
      (unpacked.head.data.toList.drop(1000).toSet === Set(0 toByte))
}
