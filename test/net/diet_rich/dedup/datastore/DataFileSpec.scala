// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.datastore

import java.io.RandomAccessFile
import net.diet_rich.dedup.backup.TestUtilites
import net.diet_rich.dedup.database.Print
import net.diet_rich.util.io._
import net.diet_rich.util.vals.{Position => Pos, _}
import org.specs2.mutable.SpecificationWithJUnit
import scala.util.Random

class DataFileSpec extends SpecificationWithJUnit {

  val base = new java.io.File("temp/tests/DataFileSpec")  
  val samplePrint = Print(Random.nextLong)
  val sampleData = Array[Byte](4,1,63,23,52,-123,3,2,-23)
  def file(name: String) = new RandomAccessFile(base.child(name), "rw")

  TestUtilites.clearDirectory(base)
  base.mkdirs()
  
  "A data file object" should {
    "have a function to write the current data print to position 8" in {
      using(file("writePrint")) { file =>
        DataFile(file, samplePrint).writePrint
        file.seek(8)
        file.readLong() === samplePrint.value
      }
    }
    "have a function to write new data to position n + 16 that returns an updated data print" in {
      using(file("writeNewData")) { file =>
        val inputData = Bytes(Array[Byte](4, 1, 63, 23, 52, -123, 3, 2,-23), Pos(2), Size(6))
        val expected  = Array[Byte](0, 0, 63, 23, 52, -123, 3, 2,  0)
        val read = new Array[Byte](9)
        val result = DataFile(file, samplePrint).writeNewData(Pos(5), inputData, Print(23450098325L))
        file.seek(16 + 5 - 2)
        fillFrom(file, read, 0, 9)
        expected === read and
        result.print === (samplePrint ^ Print(23450098325L))
      }
    }
  }
}