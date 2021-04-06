package dedup.cache

import java.util

class ZeroCache {
  protected var entries: util.NavigableMap[Long, Long] = new util.TreeMap[Long, Long]()

  def truncate(length: Long): Unit = {
    // If necessary, trim floor entry.
    Option(entries.floorEntry(length)).foreach { case Entry(storedPosition, storedLength) =>
      val distance = length - storedPosition
      if (distance < storedLength) entries.put(storedPosition, distance)
    }
    // Remove higher entries.
    entries = entries.headMap(length, false)
  }

  def write(offset: Long, length: Long): Unit = {
    // If necessary, trim floor entry.
    Option(entries.floorEntry(offset)).foreach { case Entry(storedPosition, storedLength) =>
      val distance = offset - storedPosition
      if (distance < storedLength) entries.put(storedPosition, distance)
    }

    // If necessary, trim higher entries.
    def trimHigher = Option(entries.higherEntry(offset)).map { case Entry(storedPosition, storedLength) =>
      val overlap = offset + length - storedPosition
      if (overlap > 0) {
        entries.remove(storedPosition)
        if (overlap < storedLength) {
          entries.put(storedPosition + overlap, storedLength - overlap)
          false
        } else true
      } else false
    }
    while(trimHigher.contains(true)){/**/}

    // Store new entry.
    entries.put(offset, length)
  }
}
