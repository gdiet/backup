package net.diet_rich

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

  type StringMap = String Map String
}
