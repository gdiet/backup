// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

import java.util.zip.Deflater

import net.diet_rich.dedup.util.Bytes

trait StoreMethod extends IntValue {
  def pack(data: Iterator[Bytes]): Iterator[Bytes]
  def unpack(data: Iterator[Bytes]): Iterator[Bytes]
}

object StoreMethod {
  private abstract class Valued(val value: Int) extends StoreMethod

  def apply(value: Int) = value match {
    case 0 => STORE
    case 1 => DEFLATE
  }

  val STORE: StoreMethod = new Valued(0){
    def pack(data: Iterator[Bytes]) = data
    def unpack(data: Iterator[Bytes]) = data
  }

  val DEFLATE: StoreMethod = new Valued(1){
    def pack(data: Iterator[Bytes]) = {
      val deflater = new Deflater(Deflater.BEST_COMPRESSION, true)
      new Iterator[Bytes] {
        def hasNext: Boolean = ???
        def next(): Bytes = ???
      }
    }
    def unpack(data: Iterator[Bytes]) = ???
  }
}
