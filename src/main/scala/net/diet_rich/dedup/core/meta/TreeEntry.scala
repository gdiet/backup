package net.diet_rich.dedup.core.meta

case class TreeEntry (id: Long, parent: Long, name: String, changed: Option[Long] = None, data: Option[Long] = None, deleted: Option[Long] = None)