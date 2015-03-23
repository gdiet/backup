package net.diet_rich.dedup.davserver

import java.io.InputStream
import java.lang.{Long => LongJ}
import java.util.{List => ListJ}

import scala.collection.JavaConverters.seqAsJavaListConverter

import io.milton.http.Request
import io.milton.resource._

import net.diet_rich.dedup.core.{Source, RepositoryReadWrite}
import net.diet_rich.dedup.core.meta.{TreeEntry, MetaBackend}

trait DirectoryResource extends AbstractResource with CollectionResource {
  def metaBackend: MetaBackend
  def makeResource: TreeEntry => Resource

  override final def child(childName: String): Resource = log.call(s"child(childName: '$childName')") {
    metaBackend childWarn(treeEntry.id, childName) map makeResource orNull
  }
  override final def getChildren: ListJ[_ <: Resource] = log.call("getChildren") {
    metaBackend.childrenWarn(treeEntry.id).map(makeResource).asJava
  }
}

// TODO have a look at FolderResource
case class DirectoryResourceReadOnly(treeEntry: TreeEntry, makeResource: TreeEntry => Resource, metaBackend: MetaBackend) extends DirectoryResource {
  override val writeEnabled = false
}

case class DirectoryResourceReadWrite(treeEntry: TreeEntry, resourceFactory: DedupResourceFactoryReadWrite, repository: RepositoryReadWrite)
  extends AbstractWriteResource with DirectoryResource with DeletableCollectionResource with MakeCollectionableResource with PutableResource {
  override def metaBackend: MetaBackend = repository.metaBackend
  override def makeResource: TreeEntry => Resource = resourceFactory.resource
  override val writeEnabled: Boolean = true

  override def isLockedOutRecursive(request: Request): Boolean = false

  override final def createCollection(newName: String): CollectionResource = log.call(s"createCollection($newName)") {
    val newEntry = metaBackend create (treeEntry id, newName)
    resourceFactory directory TreeEntry(newEntry, treeEntry id, newName)
  }

  override final def createNew(newName: String, inputStream: InputStream, length: LongJ, contentType: String): Resource = log.call(s"createNew($newName, in, $length, $contentType)"){
    val source = Source from (inputStream, length)
    val newEntry = repository createOrReplace (treeEntry.id, newName, Some(source))
    resourceFactory resource TreeEntry(newEntry, treeEntry id, newName)
  }
}
