// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import java.io.InputStream
import java.lang.{Long => LongJ}
import java.util.{List => ListJ, Date}
import io.milton.http.Request

import scala.collection.JavaConverters.seqAsJavaListConverter

import io.milton.resource.{CollectionResource, Resource, DeletableCollectionResource, MakeCollectionableResource, PutableResource}

import net.diet_rich.dedup.core.{Source, FileSystem}
import net.diet_rich.dedup.core.values.{Time, Size, TreeEntry}

// TODO have a look at FolderResource
trait DirectoryResource extends AbstractResource with CollectionResource {
  val resourceFactory: DedupResourceFactory
  override final def child(childName: String): Resource = debug(s"child(childName: '$childName')") {
    fileSystem.firstChild(treeEntry.id, childName).map(resourceFactory.resourceFromTreeEntry).orNull
  }
  override final def getChildren(): ListJ[_ <: Resource] = debug("getChildren()") {
    fileSystem.firstChildren(treeEntry.id).map(resourceFactory.resourceFromTreeEntry).asJava
  }
  override final def getModifiedDate(): Date = debug("getModifiedDate()") { treeEntry.changed.map(_.asDate).orNull }
}

object DirectoryResource {
  def unapply(directoryResource: DirectoryResource): Option[(FileSystem, TreeEntry)] = Some(directoryResource.fileSystem, directoryResource.treeEntry)
}

trait DirectoryWriteResource extends DeletableCollectionResource with AbstractWriteResource with MakeCollectionableResource with PutableResource { _: DirectoryResource =>
  override final def isLockedOutRecursive(request: Request): Boolean = debug(s"isLockedOutRecursive(request: $request) for directory ${treeEntry name}, ${treeEntry id}") { false }

  override final def createCollection(newName: String): CollectionResource = debug(s"createCollection($newName)") {
    val newEntry = fileSystem createUnchecked (treeEntry.id, newName)
    resourceFactory directoryResourceFromTreeEntry newEntry
  }

  override final def createNew(newName: String, inputStream: InputStream, length: LongJ, contentType: String): Resource = debug(s"createNew($newName, in, $length, $contentType)"){
    val source = Source fromInputStream (inputStream, Size(length))
    val newEntry = fileSystem createOrReplace (treeEntry.id, newName, source, Time now())
    resourceFactory resourceFromTreeEntry newEntry
  }
}

case class DirectoryResourceReadOnly (fileSystem: FileSystem, treeEntry: TreeEntry, resourceFactory: DedupResourceFactory) extends DirectoryResource with AbstractReadResource
case class DirectoryResourceReadWrite(fileSystem: FileSystem, treeEntry: TreeEntry, resourceFactory: DedupResourceFactory) extends DirectoryResource with DirectoryWriteResource
