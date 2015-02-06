package net.diet_rich.dedup.core.tree

case class TreeEntry (id: Long, parent: Long, name: String, changed: Option[Long], data: Option[Long], deleted: Option[Long])
