package net.diet_rich.dedup.core

package object meta {
  val rootEntry = TreeEntry(0L, -1L, "")
  val separator = "/"
  def pathElements(path: String) = path split separator drop 1
}
