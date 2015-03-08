package net.diet_rich.dedup.core

package object meta {
  val metaSettingsFile      = "metasettings.txt"
  val metaVersionKey        = "database version"
  val metaVersionValue      = "2.1"
  val metaHashAlgorithmKey  = "hash algorithm"

  val rootEntry = TreeEntry(0L, -1L, "")
  val rootPath = ""
  val separator = "/"
  def pathElements(path: String) = {
    require(path == "" || path.startsWith("/"), s"malformed path: $path")
    path split separator drop 1
  }
}
