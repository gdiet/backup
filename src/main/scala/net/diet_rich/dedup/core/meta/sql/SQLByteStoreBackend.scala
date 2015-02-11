package net.diet_rich.dedup.core.meta.sql

import net.diet_rich.dedup.core.meta.{DataEntry, TreeEntry}

import scala.slick.jdbc.StaticQuery

object SQLByteStoreBackend {
  import SQLConverters.getStoreEntry

  def storeEntries(dataid: Long)(implicit session: CurrentSession): List[StoreEntry] = storeEntriesForIdQuery(dataid).list
  def createByteStoreEntry(dataid: Long, start: Long, fin: Long)(implicit session: CurrentSession): Unit = createStoreEntryUpdate(dataid, start, fin).execute

  val storeEntriesForIdQuery = StaticQuery.query[Long, StoreEntry](s"SELECT id, dataid, start, fin FROM ByteStore WHERE dataid = ? ORDER BY id ASC;")
  val createStoreEntryUpdate = StaticQuery.update[(Long, Long, Long)]("INSERT INTO ByteStore (dataid, start, fin) VALUES (?, ?, ?);")
}
