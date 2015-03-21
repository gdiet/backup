package net.diet_rich.dedup.davserver

import java.util.{List => ListJ}

import scala.collection.JavaConverters.seqAsJavaListConverter

import io.milton.resource.{CollectionResource, Resource}

import net.diet_rich.dedup.core.meta.{TreeEntry, MetaBackend}

// TODO have a look at FolderResource
class DirectoryResourceReadOnly(val treeEntry: TreeEntry, resourceFactory: TreeEntry => Resource, metaBackend: MetaBackend) extends AbstractResource with CollectionResource {
  override val writeEnabled = false

  override final def child(childName: String): Resource = log.call(s"child(childName: '$childName')") {
    metaBackend childWarn(treeEntry.id, childName) map resourceFactory orNull
  }
  override final def getChildren: ListJ[_ <: Resource] = log.call("getChildren") {
    // FIXME check for name duplicates - utiltiy method needed, use it in the rest of the project as well
    metaBackend.children(treeEntry.id).map(resourceFactory).asJava
  }
}

//object DirectoryResource {
//  // FIXME usage?
//  def unapply(directoryResource: DirectoryResource): Option[(Repository, TreeEntry)] = Some(directoryResource.repository, directoryResource.treeEntry)
//}
//
//trait DirectoryWriteResource extends DeletableCollectionResource with AbstractWriteResource with MakeCollectionableResource with PutableResource { _: DirectoryResource =>
//  import repository.metaBackend
//
//  override final def isLockedOutRecursive(request: Request): Boolean = log.call(s"isLockedOutRecursive(request: $request) for directory ${treeEntry name}, ${treeEntry id}") { false }
//
//  override final def createCollection(newName: String): CollectionResource = log.call(s"createCollection($newName)") {
//    val newEntry = metaBackend createUnchecked (treeEntry.id, newName)
//    resourceFactory directoryResourceFromTreeEntry newEntry
//  }
//
//  override final def createNew(newName: String, inputStream: InputStream, length: LongJ, contentType: String): Resource = log.call(s"createNew($newName, in, $length, $contentType)"){
//    val source = Source fromInputStream (inputStream, Size(length))
//    val newEntry = repository createOrReplace (treeEntry.id, newName, source, Time now)
//    resourceFactory resourceFromTreeEntry newEntry
//  }
//}
//
//case class DirectoryResourceReadOnly[R <: Repository] (repository: Repository, treeEntry: TreeEntry, resourceFactory: DedupResourceFactory[R]) extends DirectoryResource[R] with AbstractReadResource
//case class DirectoryResourceReadWrite[R <: Repository](repository: Repository, treeEntry: TreeEntry, resourceFactory: DedupResourceFactory[R]) extends DirectoryResource[R] with DirectoryWriteResource
