package dedup
package server

case class DataArea(start: Long, stop: Long) { def size: Long = stop - start }

/* Notes on how to read free areas from database:
   We can read the starts and stops of contiguous data areas like this:
     SELECT b1.start FROM DataEntries b1 LEFT JOIN DataEntries b2 ON b1.start = b2.stop WHERE b2.stop IS NULL ORDER BY b1.start;
   However, if we simply read all DataEntries and do the sorting in Scala,
   we are much faster (as long as we don't run out of memory).  */
class FreeAreas:
  protected var free: Seq[DataArea] = Seq()
  def set(currentFree: Seq[DataArea]): Unit = synchronized {
    require(currentFree.last.stop == Long.MaxValue, s"Last chunk doesn't stop at MAXLONG but at ${currentFree.last.stop}.")
    free = currentFree
  }
  def get(size: Long): Seq[DataArea] = synchronized {
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
      free = partialChunk.copy(start = partialChunk.start + lastSize) +: completelyFree
      completeChunks :+ partialChunk.copy(stop = partialChunk.start + lastSize)
  }
