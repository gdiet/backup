// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

package object core {
  def waitFor[T](f: Future[T]): T = Await result (f, 1 seconds)
}
