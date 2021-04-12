package dedup.cache

import dedup.now

import java.io.File
import java.util.concurrent.atomic.AtomicLong

object CombinedCacheFunctionality extends App {
  val tempDir = new File(sys.props("java.io.tmpdir") + s"/dedupfs-temp/$now")
  tempDir.getParentFile.mkdirs()
  val available = new AtomicLong(4)
  val cache = new CombinedCache(available, tempDir.toPath, 10)

  // Not up to date anymore - this was written when the return type of read() was different.

  println("\nRead empty cache:")
  println(s"Holes: ${cache.read(0, 10).mkString(",")}")

  println("\nWrite beyond end-of-file:")
  cache.write(14, Array[Byte](1,2))
  println(s"Holes: ${cache.read(0, 16).mkString(",")}")

  println("\nWrite to cache file:")
  cache.write(8, Array[Byte](3,4,5,6))
  println(s"Holes: ${cache.read(0, 16).mkString(",")}")
}
