package net.diet_rich.dedupfs

import net.diet_rich.bytestore.ByteStoreRead

package object metadata {
  type Range = (Long, Long)
  type Ranges = Seq[Range]
  val NoRanges = Seq[Range]()
  type Repository = BasicRepository[_ <: MetadataRead, _ <: ByteStoreRead]
}
