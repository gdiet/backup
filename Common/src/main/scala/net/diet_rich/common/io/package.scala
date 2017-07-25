package net.diet_rich.common

import java.io.File

package object io {
  def using[Closeable <: AutoCloseable, ReturnType] (resource: Closeable)(operation: Closeable => ReturnType): ReturnType =
    try { operation(resource) } finally { resource close() }

  implicit final class RichFile(private val file: File) extends AnyVal {
    def / (child: String): File = new File(file, child)
  }
}
