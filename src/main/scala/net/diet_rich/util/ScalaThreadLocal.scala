// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

trait ScalaThreadLocal[T] { def apply(): T }

object ScalaThreadLocal {
  def apply[T](f: => T, name: String = ""): ScalaThreadLocal[T] = 
    new ScalaThreadLocal[T] {
      val local = new ThreadLocal[T] { override def initialValue = f }
      override def apply(): T = local.get
      override def toString: String = name
    }
  
  /** Upon each apply(), the flag is checked for changes. In case of changes,
   *  the thread local is recreated.
   */
  def apply[T](f: => T, flag: => Any, name: String = ""): ScalaThreadLocal[T] = 
    new ScalaThreadLocal[T] {
      val local = new ThreadLocal[(T, Any)] { override def initialValue = (f, flag) }
      override def apply(): T =
        local.get match {
          case (result, flag) => result
          case _ => local.remove(); apply
        }
      override def toString: String = name
    }
  
  import language.implicitConversions
  implicit def unwrapThreadLocal[T](scalaThreadLocal: ScalaThreadLocal[T]): T = scalaThreadLocal()
}
