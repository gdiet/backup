package net.diet_rich.dedup

import java.text.SimpleDateFormat
import java.util.Date

package object core {
  val repositoryidKey = "repository id"
  val metaDir = "meta"
  val dataDir = "data"

  val dateFormat = new SimpleDateFormat("yyyy.MM.dd_HH.mm.ss")
  def dateStringNow = dateFormat format new Date

  type StartFin = (Long, Long)
  type Ranges = Vector[StartFin]
  val RangesNil = Vector[StartFin]()
}
