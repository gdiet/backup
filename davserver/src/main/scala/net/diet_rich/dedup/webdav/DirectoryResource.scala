// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import java.util.{List => JavaList, Date}
import scala.collection.JavaConverters.seqAsJavaListConverter

import io.milton.resource.{Resource, CollectionResource}

import net.diet_rich.dedup.core.FileSystem
import net.diet_rich.dedup.core.values.TreeEntry
import net.diet_rich.dedup.util.CallLogging

// TODO have a look at FolderResource
case class DirectoryResource(fileSystem: FileSystem, treeEntry: TreeEntry, resourceFactory: DedupResourceFactory) extends AbstractResource with CollectionResource with CallLogging {
  def child(childName: String): Resource = debug(s"child(childName: '$childName')") {
    // FIXME utiliy method firstChild
    fileSystem.children(treeEntry.id, childName).headOption.map(resourceFactory.getResourceFromTreeEntry).orNull
  }
  def getChildren(): JavaList[_ <: Resource] = debug("getChildren()") {
    // FIXME utiliy method firstChildren
    fileSystem.children(treeEntry.id).groupBy(_.name).flatMap(_._2.headOption).map(resourceFactory.getResourceFromTreeEntry).toSeq.asJava
  }
  def getModifiedDate(): Date = debug("getModifiedDate") { treeEntry.changed.map(_.asDate).orNull }
}

object DirectoryResource {
  def readonly(fileSystem: FileSystem, treeEntry: TreeEntry, resourceFactory: DedupResourceFactory) =
    new DirectoryResource(fileSystem, treeEntry, resourceFactory)
  def readwrite(fileSystem: FileSystem, treeEntry: TreeEntry, resourceFactory: DedupResourceFactory) =
    new DirectoryResource(fileSystem, treeEntry, resourceFactory) with DirectoryWriteResource
}
