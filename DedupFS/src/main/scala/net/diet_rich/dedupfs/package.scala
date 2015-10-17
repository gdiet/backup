package net.diet_rich

import net.diet_rich.bytestore.ByteStoreRead
import net.diet_rich.dedupfs.metadata.MetadataRead

package object dedupfs {
  type Repository = BasicRepository[_ <: MetadataRead, _ <: ByteStoreRead]
}
