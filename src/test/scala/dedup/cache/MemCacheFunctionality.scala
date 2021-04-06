package dedup.cache

import java.util.concurrent.atomic.AtomicLong

object MemCacheFunctionality extends App {
  import MemCache.LongDecorator
  val available = new AtomicLong(100000)
  object cache extends MemCache(available) {
    def data: Array[Byte] = {
      var result = Array[Byte]()
      entries.forEach((position: Long, data: Array[Byte]) => {
        println(s"${position} ${data.length}")
        result ++= new Array[Byte]((position - result.length).asInt) ++ data
      })
      result
    }
  }

  println("Inserting spaced data works.")
  cache.write(3, Array[Byte](1,2))
  cache.write(9, Array[Byte](3,4))
  cache.write(6, Array[Byte](5,6))
  assert(cache.data.sameElements(Array[Byte](0, 0, 0, 1, 2, 0, 5, 6, 0, 3, 4)))
  assert(100000 - available.get() == 6)

  println("Replacing an entry works.")
  cache.write(6, Array[Byte](7,8))
  assert(cache.data.sameElements(Array[Byte](0, 0, 0, 1, 2, 0, 7, 8, 0, 3, 4)))
  assert(100000 - available.get() == 6)

  println("Overwriting two entries works.")
  cache.write(4, Array[Byte](1,2,3,4,5,6))
  assert(cache.data.sameElements(Array[Byte](0, 0, 0, 1, 1, 2, 3, 4, 5, 6, 4)))
  assert(100000 - available.get() == 6)
}
