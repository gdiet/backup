// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import io.milton.http._
import io.milton.resource.DeletableResource

import net.diet_rich.dedup.database.TreeEntry
import net.diet_rich.util.CallLogging
import net.diet_rich.util.Logging

trait FileWriteResource extends DeletableResource with Logging with CallLogging {
  protected val treeEntry: TreeEntry
  protected val fileSystem: DedupFileSystem
  
  override def authorise(request: Request, method: Request.Method, auth: Auth): Boolean =
    debug(s"authorise(request: '$request', method: '$method', auth: '$auth')") { true }
  
  def delete(): Unit = debug(s"deleting tree entry ${treeEntry name}, ${treeEntry id}") {
    if (!fileSystem.markDeleted(treeEntry id)) log warn s"could not delete tree entry ${treeEntry name}, ${treeEntry id}"
  }
}
