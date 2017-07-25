package net.diet_rich

package object common {
  type StringMap = String Map String

  def init[T](t: T)(f: T => Unit): T = { f(t); t }

  final class Before[T](private val t: T) extends AnyVal { def before(f: => Unit) = { f; t } }
  def valueOf[T](t: T) = new Before(t)
}
