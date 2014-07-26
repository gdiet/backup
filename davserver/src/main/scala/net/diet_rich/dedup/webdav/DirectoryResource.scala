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
trait DirectoryResource extends AbstractResource with CollectionResource {
  val resourceFactory: DedupResourceFactory
  override final def child(childName: String): Resource = debug(s"child(childName: '$childName')") {
    // FIXME utiliy method firstChild
    fileSystem.children(treeEntry.id, childName).headOption.map(resourceFactory.getResourceFromTreeEntry).orNull
  }
  override final def getChildren(): JavaList[_ <: Resource] = debug("getChildren()") {
    // FIXME utiliy method firstChildren
    fileSystem.children(treeEntry.id).groupBy(_.name).flatMap(_._2.headOption).map(resourceFactory.getResourceFromTreeEntry).toSeq.asJava
  }
  override final def getModifiedDate(): Date = debug("getModifiedDate") { treeEntry.changed.map(_.asDate).orNull }
}

case class DirectoryResourceReadOnly (fileSystem: FileSystem, treeEntry: TreeEntry, resourceFactory: DedupResourceFactory) extends DirectoryResource with AbstractReadResource
case class DirectoryResourceReadWrite(fileSystem: FileSystem, treeEntry: TreeEntry, resourceFactory: DedupResourceFactory) extends DirectoryResource with DirectoryWriteResource
