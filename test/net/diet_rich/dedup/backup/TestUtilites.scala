// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.util.io._
import net.diet_rich.util.vals._

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

object TestUtilites extends ShouldMatchers {

  def clearRepository(repository: java.io.File) = {
    clearDirectory(repository)
    net.diet_rich.dedup.repository.Create.run(Map("-r" -> repository.toString, "-g" -> "n"))
  }

  def clearDirectory(directory: java.io.File) = {
    directory.erase
    directory.mkdirs()
    directory.isDirectory() should be (true)
    directory.list() should be === Array()
  }

  def readFile(file: java.io.File): Array[Byte] = {
    using(new java.io.RandomAccessFile(file, "r")) { source =>
      val bytes = new Array[Byte](source.length toInt)
      val read = fillFrom(source.asSeekReader, bytes, Position(0), Size(bytes.length))
      assert(readAndDiscardAll(source.asSeekReader) == Size(0), s"More bytes in file $file than expected")
      bytes
    }
  }

  def dirContentsShouldBeEqual(d1: java.io.File, d2: java.io.File): Unit = {
    (d1.isFile(), d1.isDirectory()) should equal ((d2.isFile(), d2.isDirectory()))
    if (d1.isFile()) {
      readFile(d1) should equal (readFile(d2))
      (d1.length(), d1.lastModified()) should equal ((d2.length(), d2.lastModified()))
    }
    if (d1.isDirectory()) {
      d1.list() should equal (d2.list())
      d1.listFiles().sortBy(_.getName()).zip(d2.listFiles().sortBy(_.getName()))
      .foreach { case (f1, f2) =>
        f1.getName() should equal (f2.getName())
        dirContentsShouldBeEqual(f1, f2)
      }
    }
  }
  
}