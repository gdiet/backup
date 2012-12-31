// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

object Args {
  def toMap(args: Array[String]): Map[String, String] = {
    require(args.length % 2 == 0, "args must be key/value pairs (number of args found: %s)" format args.length)
    args.sliding(2, 2).map(e => e(0) -> e(1)).toMap
  }
}
