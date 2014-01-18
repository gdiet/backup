// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import io.milton.http.ResourceFactory
import io.milton.resource.Resource
import net.diet_rich.util.Logging
import io.milton.resource.CollectionResource

class DedupResourceFactory(fileSystem: FileSystem) extends ResourceFactory with Logging {
  
  override def getResource(host: String, path: String): Resource = {
    log info s"getResource(path = $path)"
    if (path == "/")
      new AbstractResource with CollectionResource {
        def getName(): String = ""
        def child(childName: String): Resource = null
        def getChildren(): java.util.List[_ <: Resource] = java.util.Collections.emptyList()
      }
    else null
  }

}
