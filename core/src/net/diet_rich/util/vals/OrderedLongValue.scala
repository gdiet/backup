// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.vals

trait OrderedLongValue[Self <: LongValue] extends LongValue with Ordered[Self] { self: Self =>
  override final def compare(other: Self): Int = value compare other.value
}
