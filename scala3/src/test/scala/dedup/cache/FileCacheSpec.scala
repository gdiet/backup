package dedup
package cache

import org.scalatest._
import org.scalatest.freespec._

import java.util.concurrent.atomic.AtomicLong
import scala.util.Random

// No IDEA support for scalatest with scala 3? https://youtrack.jetbrains.com/issue/SCL-18644
class FileCacheSpec extends AnyFreeSpec with TestDir:

  s"Important parts of ${°[FileCache]} are tested by ${°[CacheBaseSpec]}, see there..." - {
    val cache = FileCache(testDir.toPath)
    "Scenario 1 for basic functionality" in {
      def chunk1 = new Array[Byte](10000).tap(Random(1).nextBytes)
      def chunk2 = new Array[Byte](memChunk).tap(Random(2).nextBytes)
      def chunk3 = new Array[Byte](10000).tap(Random(3).nextBytes)
      cache.write(MaxInt - 1000, chunk1)
      cache.write(MaxInt + 9000, chunk2)
      cache.write(MaxInt + 8000 + memChunk, chunk3)
      val i = cache.readData(0, MaxInt + 20000 + memChunk)
      i.foreach {
        case pos -> Left(size) => println(s"LEFT $size")
        case pos -> Right(data) => println(s"RIGHT ${data.length}")
      }
//      assert(i.next == 0 -> Left(MaxInt - 1000))
      // FIXME check result
    }
    "Scenario 2 for memory handling" in {
      // FIXME test with Runtime.getRuntime.maxMemory zeros
    }
  }
