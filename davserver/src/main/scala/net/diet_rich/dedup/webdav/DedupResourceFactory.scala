// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import io.milton.http.ResourceFactory
import io.milton.resource.CollectionResource
import io.milton.resource.Resource

import net.diet_rich.dedup.database.TreeEntry
import net.diet_rich.dedup.database.NodeType
import net.diet_rich.util.CallLogging
import net.diet_rich.util.Logging

class DedupResourceFactory(fileSystem: DedupFileSystem, writeEnabled: Boolean) extends ResourceFactory with Logging with CallLogging {
  log info s"write access is ${if (writeEnabled) "ENABLED" else "DISABLED"}."
  
  private val fileResourceFactory = if (writeEnabled) FileResource.readwrite _ else FileResource.readonly _
  private val directoryResourceFactory = if (writeEnabled) DirectoryResource.readwrite _ else DirectoryResource.readonly _
  
  override def getResource(host: String, path: String): Resource = debug(s"getResource(host: $host, path: $path)") {
    val treeEntry = fileSystem entry path
    treeEntry map getResourceFromTreeEntry getOrElse null
  }
  
  def getResourceFromTreeEntry(treeEntry: TreeEntry): Resource =
    treeEntry.dataid match {
      case None =>
        if (treeEntry.nodeType != NodeType.DIR) log warn s"tree entry ${treeEntry.id} is not of type directory as expected: ${fileSystem path treeEntry.id getOrElse "?"}"
        directoryResourceFactory(fileSystem, treeEntry, this)
      case Some(dataid) =>
        if (treeEntry.nodeType != NodeType.FILE) log warn s"tree entry ${treeEntry.id} is not of type file as expected: ${fileSystem path treeEntry.id getOrElse "?"}"
        fileResourceFactory(fileSystem, treeEntry)
    }
  
}
