// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import io.milton.http._
import io.milton.resource.DeletableCollectionResource
import net.diet_rich.dedup.database.TreeEntry
import net.diet_rich.util._

trait DirectoryWriteResource extends DeletableCollectionResource with Logging with CallLogging {
  protected val treeEntry: TreeEntry
  protected val fileSystem: DedupFileSystem
  
  override def authorise(request: Request, method: Request.Method, auth: Auth): Boolean =
    debug(s"authorise(request: '$request', method: '$method', auth: '$auth')") { true }
  
  def delete(): Unit = debug("delete()") {
    log info s"deleting directory ${treeEntry name}, ${treeEntry id}"
    if (!fileSystem.markDeleted(treeEntry id)) log warn s"could not delete directory ${treeEntry name}, ${treeEntry id}"
  }
  def isLockedOutRecursive(request: Request): Boolean = debug(s"isLockedOutRecursive(request: $request) for directory ${treeEntry name}, ${treeEntry id}") {
    false
  }
}
