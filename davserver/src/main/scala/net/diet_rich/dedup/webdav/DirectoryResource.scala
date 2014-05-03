// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import java.util.Date
import java.util.{List => JavaList}

import scala.collection.JavaConverters.seqAsJavaListConverter

import io.milton.resource._
import net.diet_rich.dedup.database.TreeEntry
import net.diet_rich.util.CallLogging

// note: extend MakeCollectionableResource to enable creating directories (and files?)
// also have a look at FolderResource
case class DirectoryResource(fileSystem: DedupFileSystem, treeEntry: TreeEntry, resourceFactory: DedupResourceFactory) extends AbstractResource with CollectionResource with CallLogging {
  val typeIdentifier: String = "Directory"
  
  def getName(): String = debug("getName()") { treeEntry name }
  def child(childName: String): Resource = debug(s"child(childName: '$childName')") {
    fileSystem child (treeEntry.id, childName) map resourceFactory.getResourceFromTreeEntry getOrElse null
  }
  def getChildren(): JavaList[_ <: Resource] = debug("getChildren()") {
    fileSystem children treeEntry.id  map resourceFactory.getResourceFromTreeEntry asJava
  }
  def getModifiedDate(): Date = debug("getModifiedDate") { treeEntry.time asDate }
}

object DirectoryResource {
  val date = new Date()
  
  def readonly(fileSystem: DedupFileSystem, treeEntry: TreeEntry, resourceFactory: DedupResourceFactory) =
    new DirectoryResource(fileSystem, treeEntry, resourceFactory)
  def readwrite(fileSystem: DedupFileSystem, treeEntry: TreeEntry, resourceFactory: DedupResourceFactory) =
    new DirectoryResource(fileSystem, treeEntry, resourceFactory) with DirectoryWriteResource
}
