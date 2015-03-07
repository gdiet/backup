package net.diet_rich.dedup.core.meta

import scala.collection.mutable

import net.diet_rich.dedup.core.StartFin
import net.diet_rich.dedup.util.init

class FreeRanges(initialRanges: Seq[StartFin], nextBlockStart: Long => Long) {
  assert(initialRanges.last._2 == Long.MaxValue)
  protected val freeRanges = init(mutable.Stack[StartFin]())(_ pushAll initialRanges.reverse)

  def nextBlock: StartFin = synchronized {
    val (start, fin) = freeRanges pop()
    val blockEnd = nextBlockStart(start + 1)
    if (fin <= blockEnd) (start, fin)
    else {
      assert(freeRanges.isEmpty == (fin == Long.MaxValue))
      freeRanges push ((blockEnd, fin))
      (start, blockEnd)
    }
  }

  def pushBack(range: StartFin): Unit = synchronized { freeRanges push range }
}
