package dedup
package cache

import java.util.concurrent.atomic.AtomicLong
import scala.util.Random

// No IDEA support for scalatest with scala 3? https://youtrack.jetbrains.com/issue/SCL-18644
class FileCacheSpec extends org.scalatest.freespec.AnyFreeSpec with TestFile:

  s"Important parts of ${°[FileCache]} are tested by ${°[CacheBaseSpec]}, see there..." - {
    val cache = FileCache(testFile.toPath)

    "Scenario 1 for basic functionality" in {
      def chunk1 = new Array[Byte](10000).tap(Random(1).nextBytes)
      def chunk2 = new Array[Byte](memChunk).tap(Random(2).nextBytes)
      def chunk3 = new Array[Byte](10000).tap(Random(3).nextBytes)
      cache.write(MaxInt - 1000, chunk1)
      cache.write(MaxInt + 9000, chunk2)
      cache.write(MaxInt + 8000 + memChunk, chunk3)
      val i = cache.readData(0, MaxInt + 20000 + memChunk)
      assert(i.next == 0 -> Left(MaxInt - 1000))
      val pos1 -> Right(dat1) = i.next : @unchecked // If it's a Left, the test will fail
      assert(pos1 == MaxInt - 1000)
      assert(dat1.length == memChunk)
      assert(dat1.take(10000).toSeq == chunk1.toSeq)
      assert(dat1.drop(10000).toSeq == chunk2.dropRight(10000).toSeq)
      val pos2 -> Right(dat2) = i.next : @unchecked // If it's a Left, the test will fail
      assert(pos2 == MaxInt - 1000 + memChunk)
      assert(dat2.length == 19000)
      assert(dat2.take(9000).toSeq == chunk2.takeRight(10000).take(9000).toSeq)
      assert(dat2.drop(9000).toSeq == chunk3.toSeq)
      assert(i.next == MaxInt + 18000 + memChunk -> Left(2000))
      assert(i.hasNext == false)
    }

    val chunk = new Array[Byte](memChunk).tap(Random(1).nextBytes)

    "Scenario 2a for memory handling: Write more data than fits in memory" in {
      // clear cache
      cache.clear(MaxInt - 2000, memChunk + 20000)
      assert(cache.readData(0L, Long.MaxValue).toSeq == Seq(0 -> Left(Long.MaxValue)))
      // write more data than fits in memory
      for (position <- 0L to Runtime.getRuntime.maxMemory by memChunk) cache.write(position, chunk)
    }

    "Scenario 2b for memory handling: Read the data just written" in {
      for (position <- 0L to Runtime.getRuntime.maxMemory by memChunk) {
        val read = cache.readData(position, memChunk)
        val pos -> data = read.next()
        assert(read.isEmpty)
        assert(pos == position)
        assert(data.map(_.toSeq) == Right(chunk.toSeq))
      }
    }

    s"When the ${°[FileCache]} is closed, the cache file is deleted" in {
      assert(testFile.exists() == true)
      cache.close()
      assert(testFile.exists() == false)
    }
  }
