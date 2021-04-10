package object dedup extends scala.util.ChainingSyntax {
  def now: Long = java.lang.System.currentTimeMillis

  /** Internal size limit for byte arrays. Set to less than 0.5 MiB to avoid problems with humongous objects in G1GC
    * and to a power of 2 so blocks align well if the fuse layer uses blocks sized a smaller power of 2. (The fuse
    * default is 4096.)
    *
    * Ideally TODO the fuse options should be set so the fuse layer uses this size for file system operations, too.
    *
    * @see https://stackoverflow.com/questions/58506337/java-byte-array-of-1-mb-or-more-takes-up-twice-the-ram */
  val memChunk: Int = 2 << 18

  val hashAlgorithm = "MD5"

  /** Format a possibly large number of bytes, e.g. "27.38 MB" or "135 B". */
  def readableBytes(l: Long): String =
    if (l < 10000) "%d B".format(l)
    else if (l < 1000000) "%,.2f kB".format(l/1000d)
    else if (l < 1000000000) "%,.2f MB".format(l/1000000d)
    else if (l < 1000000000000L) "%,.2f GB".format(l/1000000000d)
    else "%,.2f T".format(l/1000000000000d)
}
