package dedup
package server

private case class Chunk(start: Long, stop: Long) { def size: Long = stop - start }

class FreeAreas:
  protected var free: Seq[Chunk] = Seq()
  def set(currentFree: Seq[Chunk]): Unit = synchronized {
    require(currentFree.last.stop == Long.MaxValue, s"Last chunk doesn't stop at MAXLONG but at ${currentFree.last.stop}.")
    free = currentFree
  }
  def get(size: Long): Seq[Chunk] = synchronized {
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
