// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

case class Hash(value: Array[Byte]) {
  def !==(a: Hash) = ! ===(a)
  def ===(a: Hash) = java.util.Arrays.equals(value, a.value)
  override def equals(a: Any) = throw new UnsupportedOperationException("use === to compare hash contents")
  override def toString = s"Hash(${value map ("%02X" format _) mkString})"
}
