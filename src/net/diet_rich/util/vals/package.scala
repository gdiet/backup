// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

package object vals {

  // workaround for https://issues.scala-lang.org/browse/SI-6943
  // use AnyRef here to look for equals problems
  // use AnyVal to have real value classes
  type AnyBase = AnyVal

}