package dedup
package store

extension(p: (String, Long, Int))
  def path = p._1

extension(l: LazyList[(Long, Array[Byte])])
  def _seq = l.map((pos, data) => pos -> data.toSeq)
