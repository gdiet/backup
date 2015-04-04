package net.diet_rich.dedup.core.meta.sql

import net.diet_rich.dedup.core.data.Print
import net.diet_rich.dedup.core.meta.{DataEntry, TreeEntry}

import scala.slick.jdbc.{GetResult, SetParameter}

object SQLConverters {
  implicit val setPrint      = SetParameter((v: Print, p) => p setLong v.value)
  implicit val getPrint      = GetResult(r => Print(r nextLong()))
  implicit val setByteArray  = SetParameter((v: Array[Byte], p) => p setBytes v)
  implicit val getByteArray  = GetResult(r => r nextBytes())
  implicit val getDataEntry  = GetResult(r => DataEntry(r <<, r <<, r <<, r <<, r <<))
  implicit val getStoreEntry = GetResult(r => StoreEntry(r <<, r <<, r <<, r <<))
  implicit val getTreeEntry  = GetResult(r => TreeEntry(r <<, r <<, r <<, r <<, r <<, r <<))
}
