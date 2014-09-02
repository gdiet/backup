// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import java.io.{InputStream, OutputStream}
import java.lang.{Long => LongJ}
import java.util.{Date, Map => JavaMap}

import scala.collection.JavaConverters.mapAsScalaMapConverter

import io.milton.http.{Auth, Range}
import io.milton.resource.{ReplaceableResource, GetableResource}

import net.diet_rich.dedup.core.{Source, FileSystem}
import net.diet_rich.dedup.core.values.{Time, Size, TreeEntry}

// TODO: FileResource is immutable. Do  we want to reflect changes to file system entries?
trait FileResource extends AbstractResource with GetableResource {
  override final def getContentLength(): LongJ = debug("getContentLength") { treeEntry.data flatMap fileSystem.sizeOf map (_.value) getOrElse[Long] 0L }
  override final def getContentType(accepts: String): String = debug(s"getContentType(accepts: $accepts)") {
    assume(accepts == null || accepts.split(",").contains("application/octet-stream")) // TODO remove?
    "application/octet-stream"
  }
  override final def getMaxAgeSeconds(auth: Auth): LongJ = debug(s"getMaxAgeSeconds(auth: $auth)") {
    // TODO check:
    // Cache for 24 hours. If we have problems with outdated content cached in read/write mode,
    // this is the place to look first. (return null if caching is not allowed.)
    60*60*24
  }
  override final def sendContent(out: OutputStream, range: Range, params: JavaMap[String, String], contentType: String) =
    debug(s"sendContent(out, range: $range, params: ${params asScala}, contentType: $contentType)") {
      require(contentType == "application/octet-stream")
      require(range == null)
      treeEntry.data foreach { fileSystem read _ foreach (out write _) }
    }
}

case class FileResourceReadOnly(fileSystem: FileSystem, treeEntry: TreeEntry) extends FileResource with AbstractReadResource

case class FileResourceReadWrite(fileSystem: FileSystem, treeEntry: TreeEntry) extends FileResource with AbstractWriteResource with ReplaceableResource {
  override final def replaceContent(inputStream: InputStream, length: LongJ): Unit = debug(s"replaceContent(in, $length)") {
    val source = Source fromInputStream (inputStream, Size(length))
    fileSystem createOrReplace (treeEntry.parent, treeEntry.name, source, Time now())
  }
}
