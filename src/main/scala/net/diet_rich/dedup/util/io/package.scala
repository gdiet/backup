package net.diet_rich.dedup.util

import java.io.File

import scala.io.Source

package object io {
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

  implicit class RichFile(val file: File) extends AnyVal {
    def / (child: String): File = new File(file, child)
    def withParentsMade(): File = init(file) { _.getParentFile.mkdirs() }
  }

}
