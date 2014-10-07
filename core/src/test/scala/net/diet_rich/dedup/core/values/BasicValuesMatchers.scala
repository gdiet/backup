// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

import org.specs2.matcher.{Matcher, Matchers}

trait BasicValuesMatchers { _: Matchers =>
  def beTheSameAs(other: Bytes): Matcher[Bytes] = beTypedEqualTo(other.toByteArray) ^^ ((_:Bytes).toByteArray aka "the actual data")
}
