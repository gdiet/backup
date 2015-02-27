package net.diet_rich.dedup

package object core {
  val repositoryIDKey = "repository id"
  type StartFin = (Long, Long)
  type Ranges = Vector[StartFin]
  val RangesNil = Vector[StartFin]()
}
