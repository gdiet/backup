package net.diet_rich.common

import java.io.File

package object io {
  def using[Closeable <: AutoCloseable, ReturnType] (resource: Closeable)(operation: Closeable => ReturnType): ReturnType =
    try { operation(resource) } finally { resource close() }

  implicit final class RichFile(private val file: File) extends AnyVal {
    def / (child: String): File = new File(file, child)
  }

  def writeSettingsFile(file: File, settings: StringMap): Unit = {
    file setWritable (true, true)
    using(new java.io.PrintWriter(file, "UTF-8")){ writer =>
      settings map { case (key, value) => s"$key = $value" } foreach writer.println
    }
    file setWritable (false, false)
  }
}
