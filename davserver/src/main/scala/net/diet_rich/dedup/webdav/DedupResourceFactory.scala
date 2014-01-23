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
  
  override def getResource(host: String, path: String): Resource = debug(s"getResource(host: $host, path: $path)") {
    val treeEntry = fileSystem entry path
    treeEntry map getResourceFromTreeEntry getOrElse null
  }
  
  private def getResourceFromTreeEntry(treeEntry: TreeEntry): Resource =
    treeEntry.dataid match {
      case None =>
        if (treeEntry.nodeType != NodeType.DIR) log warn s"tree entry ${treeEntry.id} is not a directory as expected: ${fileSystem path treeEntry.id getOrElse "?"}"
        val name: String = treeEntry.name
        val childForName: String => Option[Resource] = childName => fileSystem child (treeEntry.id, childName) map getResourceFromTreeEntry
        val children: () => Seq[Resource] = () => fileSystem children treeEntry.id map getResourceFromTreeEntry
        new DirectoryResource(name, childForName, children)
      case Some(dataid) =>
        if (treeEntry.nodeType != NodeType.FILE) log warn s"tree entry ${treeEntry.id} is not a file as expected: ${fileSystem path treeEntry.id getOrElse "?"}"
        val dataEntry = fileSystem.dataEntry(dataid)
        fileResourceFactory(fileSystem, treeEntry, dataEntry.size.value, fileSystem.bytes(dataid, dataEntry.method))
    }
  
}
