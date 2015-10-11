package net.diet_rich.common

trait ResultIterator[T] extends Iterator[T] {
  protected def iteratorName: String
  protected def expectNoMoreResults = { _: Any =>
    if (hasNext) throw new IllegalStateException(s"Expected no further results for $iteratorName.")
  }
  def nextOption(): Option[T] = if (hasNext) Some(next()) else None
  def nextOnly(): T = init(next())(expectNoMoreResults)
  def nextOptionOnly(): Option[T] = init(nextOption())(expectNoMoreResults)
}
