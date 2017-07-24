package net.diet_rich.common

package object io {
  def using[Closeable <: AutoCloseable, ReturnType] (resource: Closeable)(operation: Closeable => ReturnType): ReturnType =
    try { operation(resource) } finally { resource close() }
}
