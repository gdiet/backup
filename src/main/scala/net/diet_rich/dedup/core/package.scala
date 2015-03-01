package net.diet_rich.dedup

package object core {
  val repositoryidKey = "repository id"
  val metaDir = "meta"
  val dataDir = "data"

  type StartFin = (Long, Long)
  type Ranges = Vector[StartFin]
  val RangesNil = Vector[StartFin]()
}
