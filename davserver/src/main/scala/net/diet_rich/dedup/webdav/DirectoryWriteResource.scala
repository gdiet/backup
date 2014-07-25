// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import java.io.InputStream
import java.lang.{Long => LongJ}

import io.milton.http.Request
import io.milton.resource.{Resource, PutableResource, CollectionResource, MakeCollectionableResource, DeletableCollectionResource}

import net.diet_rich.dedup.core.Source
import net.diet_rich.dedup.core.values.{Time, Size}

trait DirectoryWriteResource extends DeletableCollectionResource with AbstractWriteResource with MakeCollectionableResource with PutableResource { _: DirectoryResource =>
  override final def isLockedOutRecursive(request: Request): Boolean = debug(s"isLockedOutRecursive(request: $request) for directory ${treeEntry name}, ${treeEntry id}") {
    false
  }

  override final def createCollection(newName: String): CollectionResource = {
    val newEntry = fileSystem createUnchecked (treeEntry.id, newName)
    resourceFactory getDirectoryResourceFromTreeEntry newEntry
  }

  final def createNew(newName: String, inputStream: InputStream, length: LongJ, contentType: String): Resource = debug(s"createNew($newName, in, $length, $contentType)"){
    val source = Source fromInputStream (inputStream, Size(length))
    // FIXME needs create-or-overwrite
    val newEntry = fileSystem storeUnchecked (treeEntry.id, newName, source, Time now())
    resourceFactory getResourceFromTreeEntry newEntry
  }
}
