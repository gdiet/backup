package net.diet_rich.dedup.core

package object meta {
  val metaSettingsFile      = "metasettings.txt"
  val metaStatusFile        = "metastatus.txt"
  val metaBackupDir         = "backup"
  val metaTimestampKey      = "last changed"
  val metaCommentKey        = "change comment"
  val metaVersionKey        = "database version"
  val metaVersionValue      = "2.1"
  val metaHashAlgorithmKey  = "hash algorithm"

  val rootEntry = TreeEntry(0L, -1L, "", None)
  val rootPath = ""
  val separator = "/"
  def pathElements(path: String) = {
    require(path == rootPath || path.startsWith(separator), s"malformed path: $path")
    path split separator drop 1
  }
}
