package net.diet_rich.util

import scala.reflect.{ClassTag, classTag}

sealed trait ScalaThreadLocal[T] { def apply(): T }

class ArmThreadLocal[T: ClassTag](factory: () => T, doClosing: Vector[T] => Unit)
  extends ScalaThreadLocal[T] with AutoCloseable { self =>
  private var isOpen = true
  private var allResources = Vector.empty[T]
  private val local = new ThreadLocal[T] {
    override def initialValue: T = self.synchronized {
      require (isOpen, s"Resource controlled ThreadLocal for ${classTag[T].runtimeClass.getName} is already closed.")
      init(factory()) (allResources :+= _)
    }
  }
  override def apply(): T = local.get
  override def toString: String = apply().toString
  override def close(): Unit = self.synchronized {
    if (isOpen) doClosing(allResources)
    isOpen = false
  }
}

object ScalaThreadLocal {
  def apply[T](factory: () => T): ScalaThreadLocal[T] =
    new ScalaThreadLocal[T] {
      val local = new ThreadLocal[T] { override def initialValue: T = factory() }
      override def apply(): T = local.get
      override def toString: String = apply().toString
    }

  def arm[T <: AutoCloseable : ClassTag](factory: () => T): ArmThreadLocal[T] =
    new ArmThreadLocal[T](factory, _ foreach(_ close()))
}
