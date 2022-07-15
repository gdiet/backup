package dedup
package server

class Level2Spec extends org.scalatest.freespec.AnyFreeSpec:
  class Writer:
    var chunks: Vector[(Long, Seq[Byte])] = Vector()
    def write(position: Long, data: Array[Byte]): Unit = chunks :+= (position, data.toSeq)
  "The level 2 write algorithm used when persisting asynchronously should" - {
    "fail when the data size is less than the reserved size" in {
      intercept[IllegalArgumentException](
        Level2.writeAlgorithm(Iterator(0L -> Array[Byte](1,2,3)), Seq(DataArea(100, 110)), Writer().write)
      )
    }
    "fail when the data size is more than the reserved size" in {
      intercept[IllegalArgumentException](
        Level2.writeAlgorithm(Iterator(0L -> Array[Byte](1,2,3)), Seq(DataArea(100, 102)), Writer().write)
      )
    }
    "write the data to the areas specified, optionally split up into more chunks" in {
      val writer = Writer()
      Level2.writeAlgorithm(
        Iterator(0L -> Array[Byte](1,2,3), 3L -> Array[Byte](4,5,6)),
        Seq(DataArea(100, 102), DataArea(200, 203), DataArea(300, 301)),
        writer.write
      )
      assert(writer.chunks ==
        Vector(100 -> Seq[Byte](1,2), 200 -> Seq[Byte](3), 201 -> Seq[Byte](4,5), 300 -> Seq[Byte](6))
      )
    }
  }
