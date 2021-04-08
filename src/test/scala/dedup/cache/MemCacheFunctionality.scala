package dedup.cache

import java.util.concurrent.atomic.AtomicLong

object MemCacheFunctionality extends App {
  val available = new AtomicLong(100000)
  object cache extends MemCache(available) {
    def data: Array[Byte] = {
      var result = Array[Byte]()
      entries.forEach((position: Long, data: Array[Byte]) => {
        result ++= new Array[Byte]((position - result.length).asInt) ++ data
      })
      result
    }
  }

  println("Inserting spaced data works.")
  assert(cache.write(3, Array[Byte](1,2)))
  assert(cache.write(9, Array[Byte](3,4)))
  assert(cache.write(6, Array[Byte](5,6)))
  assert(cache.data.sameElements(Array[Byte](0, 0, 0, 1, 2, 0, 5, 6, 0, 3, 4)))
  assert(100000 - available.get() == 6, s"${100000 - available.get()}")

  println("Replacing an entry works.")
  assert(cache.write(6, Array[Byte](7,8)))
  assert(cache.data.sameElements(Array[Byte](0, 0, 0, 1, 2, 0, 7, 8, 0, 3, 4)))
  assert(100000 - available.get() == 6)

  println("Overwriting multiple entries works.")
  assert(cache.write(4, Array[Byte](1,2,3,4,5,6)))
  assert(cache.data.sameElements(Array[Byte](0, 0, 0, 1, 1, 2, 3, 4, 5, 6, 4)))
  assert(100000 - available.get() == 8)

  println("Inserting in front works.")
  assert(cache.write(0, Array[Byte](1,2,3)))
  assert(cache.data.sameElements(Array[Byte](1, 2, 3, 1, 1, 2, 3, 4, 5, 6, 4)))
  assert(100000 - available.get() == 11)

  println("Partially replacing at the start works.")
  assert(cache.write(4, Array[Byte](9)))
  assert(cache.data.sameElements(Array[Byte](1, 2, 3, 1, 9, 2, 3, 4, 5, 6, 4)))
  assert(100000 - available.get() == 11)

  println("Partially replacing in the middle works.")
  assert(cache.write(6, Array[Byte](9)))
  assert(cache.data.sameElements(Array[Byte](1, 2, 3, 1, 9, 2, 9, 4, 5, 6, 4)))
  assert(100000 - available.get() == 11)

  println("Partially replacing in the end works.")
  assert(cache.write(9, Array[Byte](9)))
  assert(cache.data.sameElements(Array[Byte](1, 2, 3, 1, 9, 2, 9, 4, 5, 9, 4)))
  assert(100000 - available.get() == 11)

  println("Memory check works.")
  assert(!cache.write(0, new Array[Byte](100000 - 10)))
  val data = new Array[Byte](100000 - 11)
  assert(cache.write(0, data))
  assert(available.get() == 11)
}
