package dedup
package server

class FreeAreas(initialFree: Seq[DataArea] = Seq.empty):
  // Reference initialFree only in the constructor so it can be garbage collected. For details, see
  // https://stackoverflow.com/questions/8643219/do-constructor-arguments-get-gced/8646148#8646148
  protected var free: Seq[DataArea] = initialFree
  ensure("free.areas", free.lastOption.forall(_.stop == Long.MaxValue), s"Last chunk doesn't stop at MAXLONG but at ${free.last.stop}.")

  def reserve(size: Long): Seq[DataArea] = synchronized {
    ensure("free.areas.reserve", size > 0, s"Requested free chunk(s) for size $size.")
    var sizeOfAreasChecked = 0L
    free.span { area => sizeOfAreasChecked += area.size; sizeOfAreasChecked < size } match
      case _ -> Seq() => throw new IllegalStateException(s"free.areas.reserve - Reached free space limit reserving $size.")
      case areasNeededCompletely -> (lastToUse +: unused) =>
        if sizeOfAreasChecked == size then
          free = unused
          areasNeededCompletely :+ lastToUse
        else
          val lastSize = size - (sizeOfAreasChecked - lastToUse.size)
          free = lastToUse.drop(lastSize) +: unused
          areasNeededCompletely :+ lastToUse.take(lastSize)
  }
