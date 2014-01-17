package net.diet_rich.dedup.webdav

import io.milton.http.ResourceFactory
import io.milton.resource.Resource

class DedupResourceFactory(fileSystem: FileSystem) extends ResourceFactory {
  
  def getResource(host: String, path: String): Resource = ???

}