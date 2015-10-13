package net.diet_rich.dedupfs

package object metadata {
  type Range = (Long, Long)
  type Ranges = Seq[Range]
  val NoRanges = Seq[Range]()
}
