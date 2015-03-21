package net.diet_rich.dedup.davserver

import java.io.OutputStream
import java.lang.{Long => LongJ}
import java.util

import io.milton.http.{Auth, Range}
import io.milton.resource.{GetableResource, PropFindableResource, DigestResource}
import net.diet_rich.dedup.core.Repository
import net.diet_rich.dedup.core.data.Bytes
import net.diet_rich.dedup.core.meta.TreeEntry

trait FileResource extends DigestResource with PropFindableResource with GetableResource

class FileResourceReadOnly(val treeEntry: TreeEntry, repository: Repository) extends AbstractResource with FileResource {
  override val writeEnabled: Boolean = false // FIXME use enum

  // Cache for 24 hours. If we have problems with outdated content cached in read/write mode,
  // this is the place to look first. (return null if caching is not allowed.)
  override def getMaxAgeSeconds(auth: Auth): LongJ = 60*60*24L

  override def getContentLength: LongJ = (treeEntry.data flatMap repository.metaBackend.sizeOf).getOrElse[Long](0L)

  override def getContentType(accepts: String): String = {
    assume(accepts == null || accepts.split(",").contains("application/octet-stream")) // TODO remove?
    "application/octet-stream"
  }

  override def sendContent(out: OutputStream, range: Range, map: util.Map[String, String], contentType: String): Unit = {
    require(contentType == "application/octet-stream")
    require(range == null)
    treeEntry.data foreach { repository read _ foreach { case Bytes(data, offset, length) => out write (data, offset, length) } }
  }
}
