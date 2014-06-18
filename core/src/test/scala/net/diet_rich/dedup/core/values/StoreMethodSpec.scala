// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

import org.specs2.SpecificationWithJUnit

import net.diet_rich.dedup.util.init

class StoreMethodSpec  extends SpecificationWithJUnit { def is = s2"""
${"Tests for store compression/decompresion".title}

Compression should be possible $compress
  """

  def compress = {
    import StoreMethod.{DEFLATE => deflate}
    val randomData = init(Bytes.zero(1000)){ b => util.Random nextBytes b.data }
    val zeroData = Bytes.zero(5000)
    val data = Iterator(randomData, zeroData)
    val packed = deflate.pack(data).toList
    (packed.size === 1) and
      (packed.head.length should beBetween(1000, 1100))
  }

}