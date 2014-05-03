// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import io.milton.http.exceptions.ConflictException
import io.milton.http.Request
import io.milton.resource._
import java.io.InputStream

trait DirectoryWriteResource extends DeletableCollectionResource with AbstractWriteResource with PutableResource { _: DirectoryResource =>
  def isLockedOutRecursive(request: Request): Boolean = debug(s"isLockedOutRecursive(request: $request) for directory ${treeEntry name}, ${treeEntry id}") {
    false
  }
  def createNew(childName: String, inputStream: InputStream, length: java.lang.Long, contentType: String): Resource = debug(s"createNew(name: $childName, length: $length, type: $contentType) in directory ${treeEntry name}, ${treeEntry id}") {
    fileSystem.child(treeEntry id, childName) match {
      case None =>
        resourceFactory getResourceFromTreeEntry fileSystem.createFile(treeEntry id, childName, inputStream, length)
      case Some(entry) =>
        resourceFactory getResourceFromTreeEntry fileSystem.updateFile(entry id, fileSystem createDataEntry(inputStream, length))
    }
  }
}
