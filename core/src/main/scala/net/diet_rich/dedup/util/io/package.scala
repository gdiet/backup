// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.util

import java.io.{InputStream, RandomAccessFile, PrintWriter, File}

import scala.io.Source

import net.diet_rich.dedup.core.values.Time

package object io {

  def using[Closeable <: java.io.Closeable, ReturnType] (resource: Closeable)(operation: Closeable => ReturnType): ReturnType =
    try { operation(resource) } finally { resource close() }

  def using[ReturnType] (resource: Source)(operation: Source => ReturnType): ReturnType =
    try { operation(resource) } finally { resource close() }

  def readSettingsFile(file: File): Map[String, String] = {
    using(Source fromFile (file, "UTF-8")){
      _.getLines()
        .map(_.trim)
        .filterNot(_.isEmpty)
        .filterNot(_ startsWith "#")
        .map(_ split("[=:]", 2) map (_ trim()))
        .map { case Array(a, b) => (a, b) }
        .toMap
    }
  }

  def writeSettingsFile(file: File, settings: Map[String, String]): Unit =
    using(new PrintWriter(file, "UTF-8")){ writer =>
      settings map { case (key, value) => s"$key = $value" } foreach writer.println
    }

  implicit class RichFile(val file: File) extends AnyVal {
    def / (child: String) = new File(file, child)
    def changed: Time = Time(file lastModified)
  }
  implicit class RichRandomAccessFile(val file: RandomAccessFile) extends AnyVal {
    def readMethod = file read (_: Array[Byte], _: Int, _: Int)
  }
  implicit class RichInputStream(val in: InputStream) extends AnyVal {
    def readMethod = in read (_: Array[Byte], _: Int, _: Int)
  }
}
