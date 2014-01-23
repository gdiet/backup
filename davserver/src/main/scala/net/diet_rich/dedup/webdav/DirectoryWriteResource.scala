// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import io.milton.http.Request
import io.milton.resource.DeletableCollectionResource

trait DirectoryWriteResource extends DeletableCollectionResource with AbstractWriteResource { _: AbstractResource =>
  def isLockedOutRecursive(request: Request): Boolean = debug(s"isLockedOutRecursive(request: $request) for directory ${treeEntry name}, ${treeEntry id}") {
    false
  }
}
