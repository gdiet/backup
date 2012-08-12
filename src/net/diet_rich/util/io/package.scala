// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import scala.annotation.tailrec

package object io {

  type Reader = { def read(bytes: Array[Byte], offset: Int, length: Int): Int }
    
  def fillFrom(input: Reader, bytes: Array[Byte], offset: Int, length: Int): Int = {
    @tailrec
    def readRecurse(offset: Int): Int = {
      input.read(bytes, offset, length - offset) match {
        case n if n < 1 => offset
        case n => if (offset + n == length) offset + n else readRecurse(offset + n)
      }
    }
    readRecurse(offset) - offset
  }

  def readAll(input: Reader) : Long = {
    val buffer = new Array[Byte](8192)
    @tailrec
    def readRecurse(length: Long): Long =
      input.read(buffer, 0, 8192) match {
        case n if n < 1 => length
        case n => readRecurse(length + n)
      }
    readRecurse(0)
  }
  
}
