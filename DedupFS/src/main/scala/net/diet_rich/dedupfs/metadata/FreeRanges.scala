package net.diet_rich.dedupfs.metadata

class FreeRanges(initialRanges: Ranges, nextBlockStart: Long => Long) {
  private var freeRanges = initialRanges.toList.sortBy { case (start, _) => start }

  def nextBlock: Range = synchronized {
    val ((start, fin) :: tail) = freeRanges
    freeRanges = tail
    val blockEnd = nextBlockStart(start + 1)
    if (fin <= blockEnd) (start, fin)
    else {
      freeRanges = (blockEnd, fin) :: freeRanges
      (start, blockEnd)
    }
  }

  def pushBack(range: Range): Unit = synchronized { freeRanges = range :: freeRanges }
}
