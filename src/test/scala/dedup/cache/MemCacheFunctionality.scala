package dedup.cache

import java.util.concurrent.atomic.AtomicLong

object MemCacheFunctionality extends App {
  val available = new AtomicLong(100000)
  class Cache extends MemCache(available) {
    def data: Array[Byte] = {
      var result = Array[Byte]()
      entries.forEach((position: Long, data: Array[Byte]) => {
        result ++= new Array[Byte]((position - result.length).asInt) ++ data
      })
      result
    }
  }
  val cache = new Cache

  println("Check: Inserting spaced data works.")
  assert(cache.write(3, Array[Byte](1,2)))
  assert(cache.write(9, Array[Byte](3,4)))
  assert(cache.write(6, Array[Byte](5,6)))
  assert(cache.data.sameElements(Array[Byte](0, 0, 0, 1, 2, 0, 5, 6, 0, 3, 4)))
  assert(100000 - available.get() == 6, s"${100000 - available.get()}")

  println("Check: Replacing an entry works.")
  cache.clear(6, 2)
  assert(cache.write(6, Array[Byte](7,8)))
  assert(cache.data.sameElements(Array[Byte](0, 0, 0, 1, 2, 0, 7, 8, 0, 3, 4)))
  assert(100000 - available.get() == 6, s"${100000 - available.get()}")

  println("Check: Overwriting multiple entries works.")
  cache.clear(4, 6)
  assert(cache.write(4, Array[Byte](1,2,3,4,5,6)))
  assert(cache.data.sameElements(Array[Byte](0, 0, 0, 1, 1, 2, 3, 4, 5, 6, 4)))
  assert(100000 - available.get() == 8, s"${100000 - available.get()}")

  println("Check: Inserting in front works.")
  assert(cache.write(0, Array[Byte](1,2,3)))
  assert(cache.data.sameElements(Array[Byte](1, 2, 3, 1, 1, 2, 3, 4, 5, 6, 4)))
  assert(100000 - available.get() == 11, s"${100000 - available.get()}")

  println("Check: Partially replacing at the start works.")
  cache.clear(4, 1)
  assert(cache.write(4, Array[Byte](9)))
  assert(cache.data.sameElements(Array[Byte](1, 2, 3, 1, 9, 2, 3, 4, 5, 6, 4)))
  assert(100000 - available.get() == 11)

  println("Check: Partially replacing in the middle works.")
  cache.clear(6, 1)
  assert(cache.write(6, Array[Byte](9)))
  assert(cache.data.sameElements(Array[Byte](1, 2, 3, 1, 9, 2, 9, 4, 5, 6, 4)), s"${cache.data.mkString(", ")}")
  assert(100000 - available.get() == 11, s"${100000 - available.get()}")

  println("Check: Partially replacing in the end works.")
  cache.clear(9, 1)
  assert(cache.write(9, Array[Byte](9)))
  assert(cache.data.sameElements(Array[Byte](1, 2, 3, 1, 9, 2, 9, 4, 5, 9, 4)))
  assert(100000 - available.get() == 11, s"${100000 - available.get()}")

  println("Check: Memory check works.")
  assert(!cache.write(0, new Array[Byte](100000 - 10)))
  val data = new Array[Byte](100000)
  cache.clear(0, 100000)
  assert(cache.write(0, data))
  assert(available.get() == 0, s"${available.get()}")
}
