package net.diet_rich.common

trait ScalaThreadLocal[T] { def apply(): T }

object ScalaThreadLocal {
  def apply[T](f: => T): ScalaThreadLocal[T] =
    new ScalaThreadLocal[T] {
      val local = new ThreadLocal[T] { override def initialValue = f }
      override def apply(): T = local.get
      override def toString: String = apply().toString
    }
}
