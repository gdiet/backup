// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.util

class ThreadSpecific[T](f: => T) {
  private val local = new ThreadLocal[T] { override def initialValue = f }
  def apply(): T = local.get
}

object ThreadSpecific {
  def apply[T](f: => T): ThreadSpecific[T] = new ThreadSpecific(f)
  import scala.language.implicitConversions
  implicit def unwrapThreadSpecific[T](t: ThreadSpecific[T]): T = t()
}
