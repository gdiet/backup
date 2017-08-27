package net.diet_rich

import scala.collection.TraversableLike

package object common {
  type StringMap = String Map String

  def init[T](t: T)(f: T => Unit): T = { f(t); t }

  final class Before[T](private val t: T) extends AnyVal { def before(f: => Unit): T = { f; t } }
  def valueOf[T](t: T) = new Before(t)

  implicit final class RichTraversableLike[T, Repr](val trav: TraversableLike[T, Repr]) extends AnyVal {
    def maxOptionBy[B](f: T => B)(implicit cmp: Ordering[B]): Option[T] = if (trav.isEmpty) None else Some(trav maxBy f)
  }
}
