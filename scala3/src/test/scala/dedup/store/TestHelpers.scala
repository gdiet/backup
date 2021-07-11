package dedup
package store

extension(l: LazyList[(Long, Array[Byte])])
  def _seq = l.map((pos, data) => pos -> data.toSeq)
