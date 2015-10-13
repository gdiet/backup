package net.diet_rich.dedupfs.metadata

case class DataEntry (id: Long, size: Long, print: Long, hash: Array[Byte], method: Int)
