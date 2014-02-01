// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import java.io.OutputStream
import java.lang.{Long => JavaLong}
import java.util.Date
import java.util.{Map => JavaMap}

import scala.collection.JavaConverters.mapAsScalaMapConverter

import io.milton.http.Auth
import io.milton.http.Range
import io.milton.resource.GetableResource
import net.diet_rich.dedup.database.TreeEntry
import net.diet_rich.util.io.EnhancedByteSource

// Note: Extend MakeCollectionableResource to enable creating directories (and files?); also have a look at FolderResource
// Note: Currently, FileResource is immutable. Possibly, we want to reflect changes to file system entries?
case class FileResource(fileSystem: DedupFileSystem, treeEntry: TreeEntry) extends AbstractResource with GetableResource {
  val typeIdentifier: String = "File"
  
  private val dataEntry = fileSystem.dataEntry(treeEntry.dataid get)

  def getName(): String = debug("getName()") { treeEntry.name }
  def getContentLength(): JavaLong = debug("getContentLength()") { dataEntry.size value }
  def getContentType(accepts: String): String = debug(s"getContentType(accepts: $accepts)") {
    assume(accepts == null || accepts.split(",").contains("application/octet-stream"))
    "application/octet-stream"
  }
  def getModifiedDate(): Date = debug("getModifiedDate()") { new Date(treeEntry.time.value) }
  def getMaxAgeSeconds(auth: Auth): JavaLong = debug(s"getMaxAgeSeconds(auth: $auth)") {
    // Cache for 24 hours. If we have problems with outdated content cached in read/write mode,
    // this is the place to look first. (return null if caching is not allowed.)
    60*60*24
  }
  def sendContent(out: OutputStream, range: Range, params: JavaMap[String, String], contentType: String) =
    debug(s"sendContent(out, range: $range, params: ${params asScala}, contentType: $contentType)") {
      require(contentType == "application/octet-stream")
      require(range == null)
      fileSystem bytes (dataEntry id, dataEntry method) copyTo out
    }
}

object FileResource {
  def readonly(fileSystem: DedupFileSystem, treeEntry: TreeEntry) =
    new FileResource(fileSystem, treeEntry)
  def readwrite(fileSystem: DedupFileSystem, treeEntry: TreeEntry) =
    new FileResource(fileSystem, treeEntry) with AbstractWriteResource
}
