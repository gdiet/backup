// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.util

sealed trait Bytes {
  def data: Array[Byte]
  def offset: Int
  def length: Int
}

object Bytes {
  implicit class UpdateBytes(val u: { def update(data: Array[Byte], offset: Int, length: Int) }) extends AnyVal {
    import scala.language.reflectiveCalls
    def update(bytes: Bytes) = u.update(bytes.data, bytes.offset, bytes.length)
  }
}
