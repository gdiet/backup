package object dedup2 extends scala.util.ChainingSyntax {
  def now: Long = java.lang.System.currentTimeMillis

  // https://stackoverflow.com/questions/58506337/java-byte-array-of-1-mb-or-more-takes-up-twice-the-ram
  val memChunk = 524200 // a bit less than 0.5 MiB to avoid problems with humongous objects in G1GC

  type Data = Vector[Array[Byte]]

  /** Format a possibly large number of bytes, e.g. "27.38 MB" or "135 B". */
  def readableBytes(l: Long): String =
    if (l < 10000) "%d B".format(l)
    else if (l < 1000000) "%,.2f kB".format(l/1000d)
    else if (l < 1000000000) "%,.2f MB".format(l/1000000d)
    else if (l < 1000000000000L) "%,.2f GB".format(l/1000000000d)
    else "%,.2f T".format(l/1000000000000d)
}
