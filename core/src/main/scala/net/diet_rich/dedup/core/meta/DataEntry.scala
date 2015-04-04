package net.diet_rich.dedup.core.meta

import net.diet_rich.dedup.core.data.Print

case class DataEntry (id: Long, size: Long, print: Print, hash: Array[Byte], method: Int)
