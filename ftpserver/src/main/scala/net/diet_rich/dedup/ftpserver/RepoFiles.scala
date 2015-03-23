package net.diet_rich.dedup.ftpserver

import java.io._
import java.util

import scala.collection.JavaConversions.seqAsJavaList

import org.apache.ftpserver.ftplet.FtpFile

import net.diet_rich.dedup.core.{RepositoryReadWrite, Repository}
import net.diet_rich.dedup.core.meta.{MetaBackend, rootEntry, TreeEntry}
import net.diet_rich.dedup.util.{Logging, now, someNow}

// TODO utility to clean up orphan data entries and orphan byte store entries
trait RepoFile[R <: Repository] extends FtpFile with Logging {
  def repository: R
  def factory: TreeEntry => ActualRepoFile[R]
  def parentid: Long

  final def metaBackend: MetaBackend = repository.metaBackend
  final def parent: Option[ActualRepoFile[R]] = metaBackend entry parentid map factory
  final def pathByParent: String = (metaBackend path parentid getOrElse "???") + "/" + getName

  override final def getOwnerName: String = "backup"
  override final def getGroupName: String = "dedup"
  override final def isHidden: Boolean = false
}

trait RepoFileReadOnly extends RepoFile[Repository] {
  override final def factory = ActualRepoFileReadOnly(repository, _: TreeEntry)

  override final def isWritable: Boolean = false
  override final def isRemovable: Boolean = false
  override final def move(destination: FtpFile): Boolean = false
  override final def setLastModified(time: Long): Boolean = false
  override final def mkdir: Boolean = false
  override final def delete: Boolean = false
  override final def createOutputStream(offset: Long): OutputStream = throw new IOException("Repository is read-only")
}

trait RepoFileWriteFile extends RepoFile[RepositoryReadWrite] {
  def maxBytesToCache: Int

  final def storeLogic = repository.storeLogic

  override final def factory = ActualRepoFileReadWrite(repository, _: TreeEntry, maxBytesToCache)

  final def outputStream(offset: Long, whenWritten: Long => Unit): OutputStream = log.call(s"createOutputStream: $parentid/$getName") {
    if (offset != 0) throw new IOException("not random accessible")
    new CachingOutputStream(maxBytesToCache, source => whenWritten(storeLogic dataidFor source))
  }
}

trait VirtualRepoFile[R <: Repository] extends RepoFile[R] {
  override final def isReadable: Boolean = false
  override final def isFile: Boolean = false
  override final def isDirectory: Boolean = false
  override final def getLinkCount: Int = 0
  override final def getSize: Long = 0L
  override final def doesExist: Boolean = false
  override final def getLastModified: Long = now
  override final def listFiles: util.List[FtpFile] = util.Collections emptyList()
  override final def createInputStream(offset: Long): InputStream = throw new FileNotFoundException(s"file $getAbsolutePath is virtual")
  override final def getAbsolutePath: String = pathByParent
}

case class VirtualRepoFileReadOnly(repository: Repository, getName: String, parentid: Long)
  extends VirtualRepoFile[Repository] with RepoFileReadOnly {
}

case class VirtualRepoFileReadWrite(repository: RepositoryReadWrite, getName: String, parentid: Long, maxBytesToCache: Int)
  extends VirtualRepoFile[RepositoryReadWrite] with RepoFileWriteFile {
  override def delete: Boolean = false
  override def isRemovable: Boolean = false
  override def isWritable: Boolean = true
  override def move(destination: FtpFile): Boolean = false
  override def setLastModified(time: Long): Boolean = false
  override def mkdir(): Boolean = log.call(s"mkdir $getName in $parentid") { metaBackend.create(parentid, getName); true }
  override def createOutputStream(offset: Long): OutputStream = outputStream(offset, {
    dataid => metaBackend create (parentid, getName, dataid = Some(dataid))
  })
}

trait ActualRepoFile[R <: Repository] extends RepoFile[R] {
  def treeEntry: TreeEntry

  final def child(name: String): Option[ActualRepoFile[R]] = metaBackend childWarn (treeEntry.id, name) map factory

  override final def parentid: Long = treeEntry.parent

  override final def getName: String = treeEntry.name
  override final def isReadable: Boolean = true
  override final def isFile: Boolean = treeEntry.data isDefined
  override final def isDirectory: Boolean = !isFile
  override final def getLinkCount: Int = 1
  override final def getSize: Long = treeEntry.data flatMap metaBackend.sizeOf getOrElse 0L
  override final def doesExist: Boolean = true
  override final def getLastModified: Long = treeEntry.changed getOrElse now
  override final def listFiles: util.List[FtpFile] = seqAsJavaList (metaBackend children treeEntry.id map factory)
  override final def createInputStream(offset: Long): InputStream = log.call(s"createInputStream: $treeEntry") {
    if (offset != 0) throw new IOException("not random accessible")
    val data = treeEntry.data getOrElse (throw new IOException(s"$treeEntry is a directory, not a file"))
    repository read data asInputStream
  }
  override final def getAbsolutePath: String = log.call(s"getAbsolutePath for $treeEntry") { if (treeEntry == rootEntry) "/" else pathByParent }
}

case class ActualRepoFileReadOnly(repository: Repository, treeEntry: TreeEntry)
  extends ActualRepoFile[Repository] with RepoFileReadOnly

case class ActualRepoFileReadWrite(repository: RepositoryReadWrite, treeEntry: TreeEntry, maxBytesToCache: Int)
  extends ActualRepoFile[RepositoryReadWrite] with RepoFileWriteFile {
  override def isWritable: Boolean = treeEntry != rootEntry
  override def isRemovable: Boolean = isWritable
  override def mkdir(): Boolean = false
  override def createOutputStream(offset: Long): OutputStream = {
    if (treeEntry == rootEntry) throw new IOException(s"cannot write to root entry")
    outputStream(offset, {
      dataid => metaBackend change (treeEntry.id, parentid, getName, someNow, Some(dataid))
    })
  }
  override def setLastModified(time: Long): Boolean = log.call(s"setLastModified: $treeEntry") {
    treeEntry != rootEntry && metaBackend.change(treeEntry id, parentid, treeEntry name, Some(time), treeEntry data)
  }
  override def delete(): Boolean = log.call(s"delete: $treeEntry") {
    treeEntry != rootEntry && metaBackend.markDeleted(treeEntry.id)
  }
  override def move(destination: FtpFile): Boolean = log.call(s"move: $treeEntry to $destination") {
    treeEntry != rootEntry && (destination match {
      case VirtualRepoFileReadWrite(_, newName, parentid, _) => metaBackend inTransaction {
        metaBackend.children(parentid, newName).isEmpty &&
          metaBackend.change(treeEntry id, parentid, newName, treeEntry changed, treeEntry data)
      }
      case _ => false
    })
  }
}
