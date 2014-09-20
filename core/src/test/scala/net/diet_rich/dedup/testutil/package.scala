// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup

import java.io.File

import net.diet_rich.dedup.core.values.Bytes
import net.diet_rich.dedup.util.init
import net.diet_rich.dedup.util.io.RichFile

package object testutil {
  val testData = init(new File("./target/testData"))(_.mkdirs)

  def newTestFile(relativePath: String): File =
    init(testData / relativePath){ file =>
      file.getParentFile mkdirs()
      file delete()
    }

  def testDir(relativePath: String): File = testData / relativePath

  def newTestDir(relativePath: String): File =
    init(testData / relativePath){ file =>
      def deleteTree(file: File): Boolean = {
        Option(file.listFiles).toSeq.flatten foreach deleteTree
        file delete()
      }
      deleteTree(file)
      file mkdirs()
    }

  implicit class ByteArrayView(val b: Array[Byte]) extends AnyVal {
    def asBytes = Bytes(b, 0, b.length)
  }
}
