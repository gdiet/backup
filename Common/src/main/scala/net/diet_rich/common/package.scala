package net.diet_rich

import scala.collection.TraversableLike
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

package object common {
  def init[T](t: T)(f: T => Unit): T = { f(t); t }

  class Before[T](val t: T) extends AnyVal { def before(f: => Unit) = { f; t } }
  def valueOf[T](t: T) = new Before(t)

  def resultOf[T](future: Future[T]): T = Await result (future, Duration.Inf)

  def now = System.currentTimeMillis()
  def someNow = Some(now)

  def systemCores = Runtime.getRuntime.availableProcessors()

  implicit class RichIterator[T] (val iterator: Iterator[T]) extends AnyVal {
    def +: (elem: T): Iterator[T] = Iterator(elem) ++ iterator
  }

  implicit class RichTraversableLike[T, Repr] (val trav: TraversableLike[T, Repr]) extends AnyVal {
    def inGroupsOf[K](f: T => K) = trav.groupBy(f).values
    def maxOptionBy[B](f: T => B)(implicit cmp: Ordering[B]): Option[T] = if (trav.isEmpty) None else Some(trav maxBy f)
    def maybeFilter[C](cond: Option[C], f: (T, C) => Boolean) = cond.fold(trav.repr)(c => trav.filter(f(_, c)))
  }

  // FIXME not needed anymore?
  implicit class RichOption[T] (val opt: Option[T]) extends AnyVal {
    def maybeFilter[C](cond: Option[C], f: (T, C) => Boolean) = cond.fold(opt)(c => opt.filter(f(_, c)))
  }

  type StringMap = String Map String
}
