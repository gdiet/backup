package net.diet_rich.dedup.core.meta.sql

import net.diet_rich.dedup.core.meta.{StoreEntry, DataEntry, TreeEntry}

import scala.slick.jdbc.{GetResult, SetParameter}

object SQLConverters {
  implicit val setByteArray = SetParameter((v: Array[Byte], p) => p setBytes v)
  implicit val getByteArray = GetResult(r => r nextBytes())
  implicit val getDataEntry = GetResult(r => DataEntry(r <<, r <<, r <<, r <<, r <<))
  implicit val getStoreEntry = GetResult(r => StoreEntry(r <<, r <<, r <<, r <<))
  implicit val getTreeEntry = GetResult(r => TreeEntry(r <<, r <<, r <<, r <<, r <<, r <<))
}
