// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import io.milton.http.ResourceFactory
import io.milton.resource.Resource
import net.diet_rich.dedup.util.{CallLogging, Logging}

class DedupResourceFactory(writeEnabled: Boolean) extends ResourceFactory with Logging with CallLogging {
  log info s"file system write access is ${if (writeEnabled) "enabled" else "disabled"}"

  override def getResource(host: String, path: String): Resource = debug(s"getResource(host: $host, path: $path)") {
    ???
  }
}
