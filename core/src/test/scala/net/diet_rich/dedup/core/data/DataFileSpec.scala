// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.data

import org.specs2.SpecificationWithJUnit

import net.diet_rich.dedup.core.values.{BasicValuesMatchers, Bytes, TreeEntry, Size}
import net.diet_rich.dedup.testutil.{newTestFile, ByteArrayView}
import org.specs2.matcher.Matcher

class DataFileSpec extends SpecificationWithJUnit with BasicValuesMatchers { def is = s2"""
${"Data file tests".title}

The standard use case create - store - close - reopen - read should be supported $standardUseCase
  """

  def standardUseCase = {
    val file = newTestFile("DataFileSpec")
    val initialDataFile = new DataFile(0L, file, readonly = false)
    val bytes = Array[Byte](65, 66, 67, 68).asBytes
    initialDataFile writeData (10, bytes, 42L)
    initialDataFile close()
    val openedAgain = new DataFile(0L, file, readonly = true)
    val read = openedAgain.read(8L, Size(8))
    openedAgain close()
    val expected = Array[Byte](0, 0, 65, 66, 67, 68, 0, 0).asBytes
    read should beTheSameAs(expected)
  }
}
