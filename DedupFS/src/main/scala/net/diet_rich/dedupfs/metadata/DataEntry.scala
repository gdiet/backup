package net.diet_rich.dedupfs.metadata

import net.diet_rich.common.vals.Print

case class DataEntry (id: Long, size: Long, print: Print, hash: Array[Byte], method: Int)
