package net.diet_rich.dedupfs

package object metadata {
  type Range = (Long, Long)
  type Ranges = Seq[Range] // TODO make this an Iterator?
  val NoRanges = Seq[Range]()
}
