// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import java.io.File

import io.milton.http.ResourceFactory
import io.milton.resource.Resource
import net.diet_rich.dedup.core.Repository
import net.diet_rich.dedup.core.values.StoreMethod
import net.diet_rich.dedup.util.{init, CallLogging, Logging}

class DedupResourceFactory(repositoryPath: String, writeEnabled: Boolean, deflate: Boolean) extends ResourceFactory with Logging with CallLogging {
  log info s"file system write access is ${if (writeEnabled) "enabled" else "disabled"}"

  val fileSystem = init(Repository.fileSystem(
    new File(repositoryPath),
    if (deflate) StoreMethod.DEFLATE else StoreMethod.STORE,
    readonly = !writeEnabled
  ))(_.setup())

  sys.addShutdownHook {
    log info "shutting down file system ..."
    fileSystem.teardown()
  }

  override def getResource(host: String, path: String): Resource = debug(s"getResource(host: $host, path: $path)") {
    ???
  }
}
