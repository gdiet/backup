package net.diet_rich.dedup.core.meta

import net.diet_rich.dedup.util.someNow

case class TreeEntry (id: Long, parent: Long, name: String, changed: Option[Long] = someNow, data: Option[Long] = None, deleted: Option[Long] = None)
