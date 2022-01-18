package dedup
package server

/* Notes on how to read free areas from database:
   We can read the starts and stops of contiguous data areas like this:
     SELECT b1.start FROM DataEntries b1 LEFT JOIN DataEntries b2 ON b1.start = b2.stop WHERE b2.stop IS NULL ORDER BY b1.start;
   However, if we simply read all DataEntries and do the sorting in Scala,
   we are much faster (as long as we don't run out of memory).  */
object FreeAreas:
  // Not as constructor so initialFree can be garbage collected after creating it.
  def apply(initialFree: Seq[DataArea]): FreeAreas =
    require(initialFree.last.stop == Long.MaxValue, s"Last chunk doesn't stop at MAXLONG but at ${initialFree.last.stop}.")
    new FreeAreas().tap(_.free = initialFree)

class FreeAreas:
  protected var free: Seq[DataArea] = Seq()
  def reserve(size: Long): Seq[DataArea] = synchronized {
    require(size > 0, s"Requested free chunk(s) for size $size.")
    var sizeOfChunks = 0L
    val completeChunks = free.takeWhile { chunk => sizeOfChunks += chunk.size; sizeOfChunks < size }
    if sizeOfChunks == size then
      val lastToUse +: newFree = free.drop(completeChunks.size)
      free = newFree
      completeChunks :+ lastToUse
    else
      val partialChunk +: completelyFree = free.drop(completeChunks.size)
      val lastSize = size - (sizeOfChunks - partialChunk.size)
      free = partialChunk.drop(lastSize) +: completelyFree
      completeChunks :+ partialChunk.take(lastSize)
  }
