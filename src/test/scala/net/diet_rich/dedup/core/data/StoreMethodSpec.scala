package net.diet_rich.dedup.core.data

import org.specs2.Specification

import net.diet_rich.dedup.util.init

class StoreMethodSpec extends Specification { def is = s2"""
${"Tests for store compression/decompresion".title}

Compression should be possible $compress
Compression and decompression should return the original data $compressDecompress
"""

  import StoreMethod._

  val randomData = init(Bytes.zero(1000)){ b => new util.Random(42) nextBytes b.data }
  val zeroData = Bytes.zero(5000)
  lazy val packed = storeCoder(DEFLATE)(Iterator(randomData, zeroData)).toList
  lazy val unpacked = restoreCoder(DEFLATE)(packed.iterator).toList

  def compress =
    packed.map(_.length) should contain(exactly(beBetween(1000, 1100)))

  def compressDecompress =
    (unpacked should haveSize(1)) and
      (unpacked.head.data.toList.take(1000) === randomData.data.toList) and
      (unpacked.head.length === 6000) and
      (unpacked.head.data.toList.drop(1000).toSet === Set(0 toByte))
}
