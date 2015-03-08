package net.diet_rich.dedup.ftpserver

import java.io.{IOException, FileNotFoundException, InputStream, OutputStream}
import java.util

import scala.collection.JavaConversions.seqAsJavaList

import org.apache.ftpserver.ftplet.FtpFile

import net.diet_rich.dedup.core.{Repository, RepositoryReadOnly}
import net.diet_rich.dedup.core.meta.{rootEntry, TreeEntry}
import net.diet_rich.dedup.util.{Logging, now}

class RepoFiles(readAccess: RepositoryReadOnly, writeAccess: Option[Repository]) extends Logging {
  protected val metaBackend = readAccess.metaBackend
  assert(writeAccess.isEmpty || writeAccess == Some(readAccess))

  trait RepoFile extends FtpFile {
    def parentid: Long
    final def parent: Option[ActualRepoFile] = metaBackend entry parentid map ActualRepoFile.apply
    def child(name: String): Option[ActualRepoFile]

    override final def getOwnerName: String = "backup"
    override final def getGroupName: String = "dedup"
    override final def isHidden: Boolean = false
    override final def isWritable: Boolean = writeAccess isDefined
    override final def getAbsolutePath: String = (metaBackend path parentid getOrElse "???") + "/" + getName

    override def move(destination: FtpFile): Boolean = false
    override def setLastModified(time: Long): Boolean = false
    override def createOutputStream(offset: Long): OutputStream = throw new IOException("file system is read-only")
    override def mkdir(): Boolean = false
    override def delete(): Boolean = false
  }

  case class VirtualRepoFile(override val parentid: Long, override val getName: String) extends RepoFile {
    override def child(name: String): Option[ActualRepoFile] = None

    override def isReadable: Boolean = false
    override def isFile: Boolean = false
    override def isDirectory: Boolean = false
    override def getLinkCount: Int = 0
    override def getSize: Long = 0L
    override def doesExist(): Boolean = false
    override def isRemovable: Boolean = false
    override def getLastModified: Long = now
    override def listFiles(): util.List[FtpFile] = util.Collections emptyList()
    override def createInputStream(offset: Long): InputStream = throw new FileNotFoundException(s"file $getAbsolutePath is virtual")
  }

  object ActualRepoFile {
    def apply(treeEntry: TreeEntry) =
      if (writeAccess isDefined) new ActualRepoFileReadWrite(treeEntry) else new ActualRepoFileReadOnly(treeEntry)
  }

  trait ActualRepoFile extends RepoFile { def treeEntry: TreeEntry }

  class ActualRepoFileReadOnly(val treeEntry: TreeEntry) extends ActualRepoFile {
    override def parentid: Long = if (treeEntry == rootEntry) treeEntry.id else treeEntry.parent
    override def child(name: String): Option[ActualRepoFile] = {
      val children = metaBackend.children(treeEntry id, name)
      if (children.size > 1) log.warn(s"$treeEntry has multiple children with the same name $name, taking the first")
      children.headOption map ActualRepoFile.apply
    }

    override def isReadable: Boolean = log.call(s"isReadable: $treeEntry") {
      true
    }
    override def isFile: Boolean = log.call(s"isFile: $treeEntry") {
      treeEntry.data isDefined
    }
    override def isDirectory: Boolean = log.call(s"isDirectory: $treeEntry") {
      treeEntry.data isEmpty
    }
    override def getName: String = log.call(s"getName: $treeEntry") {
      treeEntry.name
    }
    override def getLinkCount: Int = log.call(s"getLinkCount: $treeEntry") {
      1
    }
    override def getSize: Long = log.call(s"getSize: $treeEntry") {
      treeEntry.data flatMap metaBackend.dataEntry map (_.size) getOrElse 0L
    }
    override def doesExist(): Boolean = log.call(s"doesExist: $treeEntry") {
      true
    }
    override def isRemovable: Boolean = log.call(s"isRemovable: $treeEntry") {
      isWritable && treeEntry != rootEntry
    }
    override def getLastModified: Long = log.call(s"getLastModified: $treeEntry") {
      treeEntry.changed getOrElse now
    }
    override def listFiles(): util.List[FtpFile] = log.call(s"listFiles: $treeEntry") {
      seqAsJavaList (metaBackend children treeEntry.id map ActualRepoFile.apply)
    }
    override def createInputStream(offset: Long): InputStream = log.call(s"createInputStream: $treeEntry") {
      if (offset != 0) throw new IOException("not random accessible")
      val data = treeEntry.data getOrElse (throw new IOException(s"$treeEntry is a directory, not a file"))
      readAccess read data asInputStream
    }
  }

  class ActualRepoFileReadWrite(treeEntry: TreeEntry) extends ActualRepoFileReadOnly(treeEntry) {
    override def delete(): Boolean = log.call(s"delete: $treeEntry") {
      metaBackend.markDeleted(treeEntry id)
    }
  }

}
