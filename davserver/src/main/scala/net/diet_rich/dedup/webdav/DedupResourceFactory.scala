// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import java.io.File

import io.milton.http.ResourceFactory
import io.milton.resource.Resource

import net.diet_rich.dedup.core.Repository
import net.diet_rich.dedup.core.values.{TreeEntry, Path, StoreMethod}
import net.diet_rich.dedup.util.{init, CallLogging, Logging}

class DedupResourceFactory(repository: File, writeEnabled: Boolean, storeMethod: StoreMethod) extends ResourceFactory with Logging with CallLogging {
  log info s"file system write access is ${if (writeEnabled) "enabled" else "disabled"}"

  private val fileSystem = init(Repository.fileSystem(repository, storeMethod, readonly = !writeEnabled))(_ setup())

  sys.addShutdownHook {
    log info "shutting down file system ..."
    fileSystem teardown()
  }

  override def getResource(host: String, path: String): Resource = debug(s"getResource(host: $host, path: $path)") {
    fileSystem.entries(Path(path)).headOption.map(resourceFromTreeEntry).orNull
  }

  def resourceFromTreeEntry(treeEntry: TreeEntry): Resource =
    treeEntry.data match {
      case None => directoryResourceFactory(fileSystem, treeEntry, this)
      case Some(dataid) => fileResourceFactory(fileSystem, treeEntry)
    }

  def directoryResourceFromTreeEntry(treeEntry: TreeEntry): DirectoryResource =
    directoryResourceFactory(fileSystem, treeEntry, this)

  private val directoryResourceFactory = if (writeEnabled) DirectoryResourceReadWrite.apply _ else DirectoryResourceReadOnly.apply _
  private val fileResourceFactory = if (writeEnabled) FileResourceReadWrite.apply _ else FileResourceReadOnly.apply _
}
