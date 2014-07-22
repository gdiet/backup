// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import java.io.OutputStream
import java.lang.{Long => JavaLong}
import java.util.{Date, Map => JavaMap}
import scala.collection.JavaConverters.mapAsScalaMapConverter

import io.milton.http.{Auth, Range}
import io.milton.resource.GetableResource

import net.diet_rich.dedup.core.FileSystem
import net.diet_rich.dedup.core.values.TreeEntry
import net.diet_rich.dedup.util.CallLogging

// TODO: FileResource is immutable. Do  we want to reflect changes to file system entries?
case class FileResource(fileSystem: FileSystem, treeEntry: TreeEntry) extends AbstractResource with GetableResource {
  def getContentLength(): JavaLong = debug("getContentLength()") { treeEntry.data flatMap fileSystem.sizeOf map (_.value) getOrElse[Long] 0L }
  def getContentType(accepts: String): String = debug(s"getContentType(accepts: $accepts)") {
    assume(accepts == null || accepts.split(",").contains("application/octet-stream")) // TODO remove?
    "application/octet-stream"
  }
  def getModifiedDate(): Date = debug("getModifiedDate()") { new Date(treeEntry.changed map (_.value) getOrElse 0L) }
  def getMaxAgeSeconds(auth: Auth): JavaLong = debug(s"getMaxAgeSeconds(auth: $auth)") {
    // TODO check:
    // Cache for 24 hours. If we have problems with outdated content cached in read/write mode,
    // this is the place to look first. (return null if caching is not allowed.)
    60*60*24
  }
  def sendContent(out: OutputStream, range: Range, params: JavaMap[String, String], contentType: String) =
    debug(s"sendContent(out, range: $range, params: ${params asScala}, contentType: $contentType)") {
      require(contentType == "application/octet-stream")
      require(range == null)
      ???
//      fileSystem bytes (dataEntry id, dataEntry method) copyTo out
    }
}

object FileResource {
  def readonly(fileSystem: FileSystem, treeEntry: TreeEntry) =
    new FileResource(fileSystem, treeEntry)
  def readwrite(fileSystem: FileSystem, treeEntry: TreeEntry) =
    new FileResource(fileSystem, treeEntry) with AbstractWriteResource
}
