package net.diet_rich.dedup.core.meta

import net.diet_rich.dedup.util._

import scala.collection.mutable

class RangesQueue(initialRanges: Seq[(Long, Long)], nextBlockStart: Long => Long) {
  assert(initialRanges.last._2 == Long.MaxValue)

  // Note: We could use a PriorityQueue here - however, it is not really necessary,
  // because here, an ordinary queue 'heals', quickly getting into the right order.
  private val freeRangesQueue = init(mutable.Queue[(Long, Long)]())(_ enqueue (initialRanges:_*))

  /** Dequeues the required size plus padding up to the next end-of-block. */
  def dequeueAtLeast(size: Long): List[(Long, Long)] = synchronized {
    @annotation.tailrec
    def collectFreeRanges(size: Long, ranges: List[(Long, Long)]): List[(Long, Long)] = {
      val (start, fin) = freeRangesQueue dequeue()
      fin - start match {
        case `size`        => (start, fin) :: ranges
        case n if n < size => collectFreeRanges(size - n, (start, fin) :: ranges)
        case n =>
          println(s"*** $size -> ${nextBlockStart(start + size)}")
          nextBlockStart(start + size) match {
            case newFin if newFin <= fin =>
              freeRangesQueue enqueue ((newFin, fin))
              (start, newFin) :: ranges
            case _ => (start, fin) :: ranges
          }
      }
    }
    if (size == 0L) Nil else collectFreeRanges(size, Nil)
  }

  def enqueue(range: (Long, Long)): Unit = synchronized { freeRangesQueue enqueue range }
  def enqueue(ranges: Seq[(Long, Long)]): Unit = synchronized { freeRangesQueue.enqueue(ranges:_*) }
}
