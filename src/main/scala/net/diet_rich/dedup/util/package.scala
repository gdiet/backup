package net.diet_rich.dedup

package object util {
  class Before[T](val t: T) extends AnyVal { def before(f: => Unit) = { f; t } }
  def valueOf[T](t: T) = new Before(t)
  def init[T](t: T)(f: T => Unit): T = { f(t); t }
  def now = System.currentTimeMillis()
  def systemCores = Runtime.getRuntime.availableProcessors()
}
