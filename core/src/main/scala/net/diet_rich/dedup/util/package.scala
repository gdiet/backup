package net.diet_rich.dedup

package object util {
  class Before[T](val t: T) extends AnyVal { def before(f: => Unit) = { f; t } }
  def valueOf[T](t: T) = new Before(t)
  def init[T](t: T)(f: T => Unit): T = { f(t); t }
  def now = System.currentTimeMillis() // FIXME always used as Some(now)?
  def systemCores = Runtime.getRuntime.availableProcessors()
  val readOnly = Writable.readOnly
  val readWrite = Writable.readWrite

  import scala.language.implicitConversions
  implicit def writableAsBoolean(writable: Writable): Boolean = writable == readWrite
}
