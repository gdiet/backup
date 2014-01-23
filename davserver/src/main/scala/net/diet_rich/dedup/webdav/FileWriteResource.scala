// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import io.milton.http.Auth
import io.milton.http.Request
import io.milton.resource.CollectionResource
import io.milton.resource.DeletableResource
import io.milton.resource.MoveableResource
import net.diet_rich.dedup.database.TreeEntry
import net.diet_rich.util.CallLogging
import net.diet_rich.util.Logging
import io.milton.http.exceptions.BadRequestException

trait FileWriteResource extends DeletableResource with MoveableResource with Logging with CallLogging {
  protected val treeEntry: TreeEntry
  protected val fileSystem: DedupFileSystem
  
  override def authorise(request: Request, method: Request.Method, auth: Auth): Boolean =
    debug(s"authorise(request: '$request', method: '$method', auth: '$auth')") { true }
  
  def delete(): Unit = debug("delete()") {
    log info s"deleting file ${treeEntry name}, ${treeEntry id}"
    if (!fileSystem.markDeleted(treeEntry id)) log warn s"could not delete file ${treeEntry name}, ${treeEntry id}"
  }
  
  def moveTo(destination: CollectionResource, newName: String): Unit = debug(s"moveTo(destination: $destination, newName: $newName)") {
    destination match {
      case dir @ DirectoryResource(`fileSystem`, dirEntry, _) =>
        if (newName isEmpty) throw new BadRequestException("rename not possible - new name is empty")
        if (newName matches ".*[\\?\\*\\/\\\\].*") throw new BadRequestException(s"rename not possible - new name '$newName' contains illegal characters, one of [?*/\\]")
        if (!fileSystem.moveRename(treeEntry id, newName, dirEntry id)) log warn s"could not move $this to $dir"
      case other => throw new BadRequestException(s"Can't move to target $other")
    }
  }
}
