// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

trait ScalaThreadLocal[T] { def apply() : T }

object ScalaThreadLocal {
  def apply[T](f : => T, name: String = "") : ScalaThreadLocal[T] = 
    new ScalaThreadLocal[T] {
      val local = new ThreadLocal[T] { override def initialValue = f }
      override def apply : T = local.get
      override def toString: String = name
    }
}
