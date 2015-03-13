package net.diet_rich.dedup.util

import java.io.{PrintWriter, File}

import scala.io.Source

package object io {
  def using[Closeable <: AutoCloseable, ReturnType] (resource: Closeable)(operation: Closeable => ReturnType): ReturnType =
    try { operation(resource) } finally { resource close() }

  def using[T] (resource: Source)(operation: Source => T): T =
    try { operation(resource) } finally { resource close() }

  def readSettingsFile(file: File): Map[String, String] = {
    using(Source fromFile (file, "UTF-8")){
      _ .getLines()
        .map(_ trim)
        .filterNot(_ isEmpty)
        .filterNot(_ startsWith "#")
        .map(_ split("[=:]", 2) map (_ trim()))
        .map{case Array(a, b) => (a, b)}
        .toMap
    }
  }

  def writeSettingsFile(file: File, settings: Map[String, String]): Unit = {
    file setWritable (true, true)
    using(new PrintWriter(file, "UTF-8")){ writer =>
      settings map { case (key, value) => s"$key = $value" } foreach writer.println
    }
    file setWritable (false, false)
  }

  implicit class RichFile(val file: File) extends AnyVal {
    def / (child: String): File = new File(file, child)
    def withParentsMade(): File = init(file) { _.getParentFile.mkdirs() }
  }

}
