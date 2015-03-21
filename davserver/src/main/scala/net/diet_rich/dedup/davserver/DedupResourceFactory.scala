package net.diet_rich.dedup.davserver

import io.milton.http.ResourceFactory
import io.milton.resource._

import net.diet_rich.dedup.core.{Repository, RepositoryReadWrite}
import net.diet_rich.dedup.core.meta.TreeEntry
import net.diet_rich.dedup.util.Logging

class DedupResourceFactoryReadOnly(repository: Repository) extends ResourceFactory with Logging {
  import repository.metaBackend
  
  override def getResource(host: String, path: String): Resource = log.call(s"getResource(host: $host, path: $path)") {
    val entries = metaBackend entries path
    if (entries.size > 1) log warn s"Taking the first of multiple entries for path $path, all entries are: $entries"
    entries.headOption map resource orNull
  }

  protected def resource(treeEntry: TreeEntry): Resource =
    treeEntry.data match {
      case None => directory(treeEntry)
      case Some(dataid) => file(treeEntry)
    }

  protected def directory(treeEntry: TreeEntry): DirectoryResource = new DirectoryResourceReadOnly(treeEntry, resource, metaBackend)
  protected def file(treeEntry: TreeEntry): FileResource = new FileResourceReadOnly(treeEntry, repository)

  def close() = repository close()
}

class DedupResourceFactoryReadWrite(repository: RepositoryReadWrite) extends DedupResourceFactoryReadOnly(repository) {
  override protected def directory(treeEntry: TreeEntry): DirectoryResource = ???
  override protected def file(treeEntry: TreeEntry): FileResource = ???
}
