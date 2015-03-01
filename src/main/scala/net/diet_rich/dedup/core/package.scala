package net.diet_rich.dedup

package object core {
  val repositoryidKey = "repository id"
  type StartFin = (Long, Long)
  type Ranges = Vector[StartFin]
  val RangesNil = Vector[StartFin]()
}
