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

class DedupResourceFactory(repositoryPath: String, writeEnabled: Boolean, deflate: Boolean) extends ResourceFactory with Logging with CallLogging {
  log info s"file system write access is ${if (writeEnabled) "enabled" else "disabled"}"

  private val fileSystem = init(Repository.fileSystem(
    new File(repositoryPath),
    if (deflate) StoreMethod.DEFLATE else StoreMethod.STORE,
    readonly = !writeEnabled
  ))(_.setup())

  sys.addShutdownHook {
    log info "shutting down file system ..."
    // FIXME at least here, the H2 db shutdown hook acts prematurely and/or concurrently with the internal db shutdown
    fileSystem.teardown()
  }

  override def getResource(host: String, path: String): Resource = debug(s"getResource(host: $host, path: $path)") {
    fileSystem.entries(Path(path)).headOption.map(getResourceFromTreeEntry).orNull
  }

  def getResourceFromTreeEntry(treeEntry: TreeEntry): Resource =
    treeEntry.data match {
      case None => directoryResourceFactory(fileSystem, treeEntry, this)
      case Some(dataid) => ??? // fileResourceFactory(fileSystem, treeEntry)
    }

  private val directoryResourceFactory = if (writeEnabled) DirectoryResource.readwrite _ else DirectoryResource.readonly _

}
