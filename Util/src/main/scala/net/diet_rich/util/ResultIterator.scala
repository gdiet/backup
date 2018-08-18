package net.diet_rich.util

import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION

trait ResultIterator[T] extends Iterator[T] {
  protected def iteratorName: String
  @elidable(ASSERTION)
  protected def expectNoMoreResults(): Unit =
    if (hasNext) throw new IllegalStateException(s"Expected no further results for $iteratorName.")
  def nextOption(): Option[T] = if (hasNext) Some(next()) else None
  /** Note that the check for no more results is elidable. */
  def nextOnly(): T = valueOf (next()) before expectNoMoreResults()
  /** Note that the check for no more results is elidable. */
  def nextOptionOnly(): Option[T] = valueOf (nextOption()) before expectNoMoreResults()
}
