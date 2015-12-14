package net.diet_rich.common

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._
import scala.reflect.{classTag, ClassTag}

trait ScalaThreadLocal[T] { def apply(): T }

class ArmThreadLocal[T: ClassTag](factory: () => T, doClosing: ConcurrentLinkedQueue[T] => Unit) extends ScalaThreadLocal[T] with AutoCloseable { self =>
  private var isOpen = true
  private val allResources = new ConcurrentLinkedQueue[T]()
  private val local = new ThreadLocal[T] {
    override def initialValue = self.synchronized {
      require (isOpen, s"Resource controlled ThreadLocal for ${classTag[T].runtimeClass.getName} is already closed.")
      init(factory()) (allResources add _)
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
  def apply[T](f: => T): ScalaThreadLocal[T] =
    new ScalaThreadLocal[T] {
      val local = new ThreadLocal[T] { override def initialValue = f }
      override def apply(): T = local.get
      override def toString: String = apply().toString
    }

  def arm[T <: AutoCloseable : ClassTag](f: () => T): ArmThreadLocal[T] =
    new ArmThreadLocal[T](f, _.asScala foreach(_ close()))
}
