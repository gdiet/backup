package dedup
package server

class Level2Spec extends org.scalatest.freespec.AnyFreeSpec:
  class Writer:
    var chunks: Vector[DataArea] = Vector()
    def write(position: Long, data: Array[Byte]): Unit =
      chunks :+= DataArea(position, position + data.length)
  "The level 2 write algorithm used when persisting asynchronously should" - {
    "fail when the data size is less than the reserved size" in {
      val writer = Writer()
      intercept[IllegalArgumentException](
        Level2.writeAlgorithm(Iterator(0L -> Array[Byte](1,2,3)), Seq(DataArea(100, 110)), writer.write)
      )
    }
    // FIXME continue
  }
