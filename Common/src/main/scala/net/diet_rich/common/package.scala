package net.diet_rich

package object common {
  def init[T](t: T)(f: T => Unit): T = { f(t); t }
  class Before[T](val t: T) extends AnyVal { def before(f: => Unit) = { f; t } }
  def valueOf[T](t: T) = new Before(t)
}
