package net.diet_rich.dedupfs

import net.diet_rich.common.Bytes
import net.diet_rich.dedupfs.metadata._

class StoreLogic {

}

trait StorePackedDataLogic {
  def writeData: (Bytes, Long) => Unit
  def freeRanges: FreeRanges

  def storePackedData(data: Iterator[Bytes]): Ranges = {
    val (finalProtocol, remaining) = data.foldLeft((NoRanges, Option.empty[Range])) { case ((protocol, range), bytes) =>
      assert (bytes.length > 0)
      write(bytes, protocol, range getOrElse freeRanges.nextBlock)
    }
    remaining foreach freeRanges.pushBack
    finalProtocol
  }

  @annotation.tailrec
  private def write(bytes: Bytes, protocol: Ranges, free: Range): (Ranges, Option[Range]) = {
    val (start, fin) = free
    val length = fin - start
    if (length >= bytes.length) {
      writeData (bytes, start)
      val rest = if (length == bytes.length) None else Some((start + bytes.length, fin))
      (normalizedAdd(protocol, start, start + bytes.length), rest)
    } else {
      writeData (bytes copy (length = length.toInt), start)
      write(bytes addOffset length.toInt, normalizedAdd(protocol, start, fin), freeRanges.nextBlock)
    }
  }

  private def normalizedAdd(protocol: Ranges, start: Long, fin: Long): Ranges =
    protocol match {
      case heads :+ ((lastStart, `start`)) => heads :+ (lastStart, fin)
      case _ => protocol :+ (start, fin)
    }
}
