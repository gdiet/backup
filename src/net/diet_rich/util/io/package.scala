// Copyright (c) 2013 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import java.io._

package object io {
  type Closeable = { def close(): Unit }
  
  def using[Closeable <: io.Closeable, ReturnType] (resource: Closeable)(operation: Closeable => ReturnType): ReturnType =
    try { operation(resource) } finally { resource.close }
  
  def readSettingsFile(path: String): Map[String, String] =
    using(scala.io.Source.fromFile(path, "UTF-8")) { source =>
      source.getLines
      .map(_.trim)
      .filterNot(_.isEmpty)
      .filterNot(_.startsWith("#"))
      .map(_.split("[=:]", 2).map(_.trim()))
      .map{case Array(a,b) => (a,b)}
      .toMap
    }

  def writeSettingsFile(file: File, settings: Map[String, String]): Unit =
    using(new PrintWriter(file, "UTF-8")) { writer =>
      writer.write(
        settings
        .map{case (key, value) => "%s = %s" format (key, value)}
        .mkString("\n")
      )
    }
  
  implicit def enhanceFile(file: File): {
    def child(child: String): File
    def erase: Boolean
  } = new Object {
    def child(child: String): File = new File(file, child)
    def erase: Boolean = (
      if (file.isDirectory()) file.listFiles.forall(_.erase) else true
    ) && file.delete
  }
}
