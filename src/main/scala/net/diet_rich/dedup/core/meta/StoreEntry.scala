package net.diet_rich.dedup.core.meta

// FIXME remove id and dataid - this just leaves start and fin, make it a tuple
case class StoreEntry (id: Long, dataid: Long, start: Long, fin: Long)
