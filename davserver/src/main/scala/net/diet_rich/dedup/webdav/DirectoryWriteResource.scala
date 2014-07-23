// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import io.milton.http.Request
import io.milton.resource.{CollectionResource, MakeCollectionableResource, DeletableCollectionResource}

trait DirectoryWriteResource extends DeletableCollectionResource with AbstractWriteResource with MakeCollectionableResource { _: DirectoryResource =>
  override final def isLockedOutRecursive(request: Request): Boolean = debug(s"isLockedOutRecursive(request: $request) for directory ${treeEntry name}, ${treeEntry id}") {
    false
  }

  override final def createCollection(newName: String): CollectionResource = {
    val newEntry = fileSystem.createUnchecked(treeEntry.id, newName)
    resourceFactory getDirectoryResourceFromTreeEntry newEntry
  }
}
