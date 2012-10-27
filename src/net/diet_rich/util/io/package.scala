// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import scala.annotation.tailrec

package object io {

  /** closes the resource after the operation */
  def using[Closeable <: io.Closeable, ReturnType] (resource: Closeable)(operation: Closeable => ReturnType): ReturnType =
    try { operation(resource) } finally { resource.close }

  type ByteSource = { def read(bytes: Array[Byte], offset: Int, length: Int): Int }
  type Closeable = { def close(): Unit }
  type Seekable = { def seek(pos: Long): Unit }
  type Reader = ByteSource with Closeable
  type SeekReader = Seekable with Reader
  
  val emptyReader: Reader = new Object {
    def read(b: Array[Byte], off: Int, len: Int): Int = 0
    def close(): Unit = Unit
  }
  
  def fillFrom(input: ByteSource, bytes: Array[Byte], offset: Int, length: Int): Int = {
    @tailrec
    def readRecurse(offset: Int): Int = {
      input.read(bytes, offset, length - offset) match {
        case n if n < 1 => offset
        case n => if (offset + n == length) offset + n else readRecurse(offset + n)
      }
    }
    readRecurse(offset) - offset
  }

  def readAndDiscardAll(input: ByteSource) : Long = {
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
