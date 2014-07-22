// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import io.milton.http.Auth
import io.milton.http.Request
import io.milton.http.exceptions.BadRequestException
import io.milton.resource.CollectionResource
import io.milton.resource.DeletableResource
import io.milton.resource.MoveableResource

trait AbstractWriteResource extends DeletableResource with MoveableResource { _: AbstractResource =>
  override def authorise(request: Request, method: Request.Method, auth: Auth): Boolean =
    debug(s"authorise(request: '$request', method: '$method', auth: '$auth')") { true }
  
  def delete(): Unit = debug("delete()") {
    log info s"deleting $treeEntry"
    if (!fileSystem.markDeleted(treeEntry id)) log warn s"could not mark deleted $treeEntry"
  }
  
  def moveTo(destination: CollectionResource, newName: String): Unit = debug(s"moveTo(destination: $destination, newName: $newName)") {
    destination match {
      case DirectoryResource(`fileSystem`, dir, _) =>
        if (newName isEmpty) throw new BadRequestException("Rename not possible: New name is empty.")
        if (newName matches ".*[\\?\\*\\/\\\\].*") throw new BadRequestException(s"Rename not possible: New name '$newName' contains illegal characters, one of [?*/\\].")
        if (!fileSystem.moveRename(treeEntry id, dir id, newName)) log warn s"Could not move $treeEntry to $dir."
      case other => throw new BadRequestException(s"Can't move $this to $other.")
    }
  }
  
}
