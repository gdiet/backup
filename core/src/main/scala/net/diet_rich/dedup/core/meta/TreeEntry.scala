package net.diet_rich.dedup.core.meta

import net.diet_rich.dedup.util.now

case class TreeEntry (id: Long, parent: Long, name: String, changed: Option[Long] = Some(now), data: Option[Long] = None, deleted: Option[Long] = None)
