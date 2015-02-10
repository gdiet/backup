package net.diet_rich.dedup.core.tree.sql

import net.diet_rich.dedup.core.tree.TreeEntry

import scala.slick.jdbc.{GetResult, SetParameter}

object SQLConverters {
  implicit val setByteArray = SetParameter((v: Array[Byte], p) => p setBytes v)
  implicit val getStoreEntry = GetResult(r => StoreEntry(r <<, r <<, r <<, r <<))
  implicit val getTreeEntry = GetResult(r => TreeEntry(r <<, r <<, r <<, r <<, r <<, r <<))
}
