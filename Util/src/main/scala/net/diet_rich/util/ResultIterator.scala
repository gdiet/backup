package net.diet_rich.util

trait ResultIterator[T] extends Iterator[T] {
  protected def iteratorName: String
  protected def expectNoMoreResults(): Unit =
    if (hasNext) throw new IllegalStateException(s"Expected no further results for $iteratorName.")
  def nextOption(): Option[T] = if (hasNext) Some(next()) else None
  def nextOnly(): T = valueOf (next()) before expectNoMoreResults()
  def nextOptionOnly(): Option[T] = valueOf (nextOption()) before expectNoMoreResults()
}
