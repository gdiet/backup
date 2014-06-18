// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

package object util {
  class Before[T](t: T) { def before(f: => Unit) = { f; t } }
  def valueOf[T](t: T) = new Before(t)
  def init[T](t: T)(f: T => Unit): T = { f(t); t }
  def resultOf[T](f: Future[T]): T = Await result (f, 1 seconds)
  def !!![T]: T = sys.error("this code should have never been reached.")
}
