package net.diet_rich.dedup.davserver

import io.milton.http.ResourceFactory
import io.milton.resource.Resource

import net.diet_rich.dedup.core.Repository
import net.diet_rich.dedup.core.meta.TreeEntry
import net.diet_rich.dedup.util.{init, Logging}

class DedupResourceFactory[R <: Repository](repository: R) extends ResourceFactory with Logging {
  import repository.metaBackend

  override def getResource(host: String, path: String): Resource = log.call(s"getResource(host: $host, path: $path)") {
    val entries = metaBackend entries path
    if (entries.size > 1) log warn s"Taking the first of multiple entries for path $path, all entries are: $entries"
    entries.headOption map resourceFromTreeEntry orNull
  }

  def resourceFromTreeEntry(treeEntry: TreeEntry): Resource =
    treeEntry.data match {
      case None => ??? // directoryResourceFactory(fileSystem, treeEntry, this)
      case Some(dataid) => ??? // fileResourceFactory(fileSystem, treeEntry)
    }

//  def directoryResourceFromTreeEntry(treeEntry: TreeEntry): DirectoryResource =
//    directoryResourceFactory(fileSystem, treeEntry, this)
//
//  private val directoryResourceFactory = if (writeEnabled) DirectoryResourceReadWrite.apply _ else DirectoryResourceReadOnly.apply _
//  private val fileResourceFactory = if (writeEnabled) FileResourceReadWrite.apply _ else FileResourceReadOnly.apply _
}
