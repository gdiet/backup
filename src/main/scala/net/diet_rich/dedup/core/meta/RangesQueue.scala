package net.diet_rich.dedup.core.meta

import net.diet_rich.dedup.core.StartFin
import net.diet_rich.dedup.util._

import scala.collection.mutable

class RangesQueue(initialRanges: Seq[StartFin], nextBlockStart: Long => Long) {
  assert(initialRanges.last._2 == Long.MaxValue)

  // Note: We could use a PriorityQueue here - however, it is not really necessary,
  // because here, an ordinary queue 'heals', quickly getting into the right order.
  private val freeRangesQueue = init(mutable.Queue[StartFin]())(_ enqueue (initialRanges:_*))

  /** Dequeue the required size plus padding up to the next end-of-block. */
  def dequeueAtLeast(size: Long): List[StartFin] = if (size == 0L) Nil else synchronized {
    @annotation.tailrec
    def collectFreeRanges(size: Long, ranges: List[StartFin]): List[StartFin] = {
      val (start, fin) = freeRangesQueue dequeue()
      fin - start match {
        case `size`        => (start, fin) :: ranges
        case n if n < size => collectFreeRanges(size - n, (start, fin) :: ranges)
        case n =>
          nextBlockStart(start + size) match {
            case newFin if newFin <= fin =>
              freeRangesQueue enqueue ((newFin, fin))
              (start, newFin) :: ranges
            case _ => (start, fin) :: ranges
          }
      }
    }
    // FIXME collection that does not need reversal
    collectFreeRanges(size, Nil).reverse
  }

  def enqueue(range: StartFin): Unit = synchronized { freeRangesQueue enqueue range }
  def enqueue(ranges: Seq[StartFin]): Unit = if (!ranges.isEmpty) synchronized { freeRangesQueue.enqueue(ranges:_*) }
}
