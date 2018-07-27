package net.diet_rich

package object util {
  def init[T](t: T)(f: T => Unit): T = { f(t); t }

  final class Before[T](private val t: T) extends AnyVal { def before(f: => Unit): T = { f; t } }
  def valueOf[T](t: T) = new Before(t)
}
