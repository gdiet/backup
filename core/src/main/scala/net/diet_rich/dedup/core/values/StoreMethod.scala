// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

case class StoreMethod (value: Int) extends IntValue {
  assume(0 <= value && value <= 1, s"Unsupported store method $value")
}

object StoreMethod {
  val STORE = StoreMethod(0)
  val DEFLATE = StoreMethod(1)
}
