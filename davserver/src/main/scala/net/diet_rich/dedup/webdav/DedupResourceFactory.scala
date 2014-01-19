// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import io.milton.http.ResourceFactory
import io.milton.resource.Resource
import net.diet_rich.util.Logging
import io.milton.resource.CollectionResource
import net.diet_rich.util.CallLogging

class DedupResourceFactory(fileSystem: FileSystem) extends ResourceFactory with CallLogging {
  
  override def getResource(host: String, path: String): Resource = info(s"getResource(host: $host, path: $path)") {
    if (path == "/")
      new AbstractResource with CollectionResource {
        def getName(): String = ""
        def child(childName: String): Resource = null
        def getChildren(): java.util.List[_ <: Resource] = java.util.Collections.emptyList()
      }
    else null
  }

}
