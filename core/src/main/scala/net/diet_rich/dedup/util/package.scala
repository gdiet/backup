// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup

import java.io.File
import java.util.concurrent.{TimeUnit, RejectedExecutionHandler, ArrayBlockingQueue, ThreadPoolExecutor}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt

package object util {
  class Before[T](t: T) { def before(f: => Unit) = { f; t } }
  def valueOf[T](t: T) = new Before(t)
  def init[T](t: T)(f: T => Unit): T = { f(t); t }
  def resultOf[T](f: Future[T]): T = Await result (f, 1 day)
  def !!![T]: T = sys.error("this code should have never been reached.")

  // Note: Idea taken from scalaz
  implicit class Equal[A](val a: A) extends AnyVal {
    def ===(b: A) = a == b
  }

  def BlockingThreadPoolExecutor(threadPoolSize: Int): ThreadPoolExecutor = {
    val executorQueue = new ArrayBlockingQueue[Runnable](threadPoolSize)
    val rejectHandler = new RejectedExecutionHandler {
      override def rejectedExecution(r: Runnable, e: ThreadPoolExecutor): Unit = executorQueue put r
    }
    new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0, TimeUnit.SECONDS, executorQueue, rejectHandler)
  }

  implicit class RichString(val string: String) extends AnyVal {
    private def processSpecialSyntax(rule1: String => String, rule2: String => String): String = {
      string.split('!').sliding(2, 2).map(_.toList).map {
        case List(a, b) => List(rule1(a), rule2(b))
        case List(a) => List(rule1(a))
        case _ => throw new IllegalStateException
      }.flatten.mkString
    }
    def preparedForRegexpMatch = processSpecialSyntax(java.util.regex.Pattern quote, identity)
    def withDateStringReplaced = processSpecialSyntax(identity, new java.text.SimpleDateFormat(_) format new java.util.Date)
  }
}
