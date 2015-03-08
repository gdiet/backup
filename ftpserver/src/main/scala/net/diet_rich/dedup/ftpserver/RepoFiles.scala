package net.diet_rich.dedup.ftpserver

import java.io.{InputStream, OutputStream}
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
    final def parent: Option[ActualRepoFile] = metaBackend entry parentid map ActualRepoFile
    def child(name: String): Option[ActualRepoFile] = ???

    override final def getOwnerName: String = "backup"
    override final def getGroupName: String = "dedup"
    override final def isHidden: Boolean = false
    override final def isWritable: Boolean = writeAccess isDefined
    override final def getAbsolutePath: String = (metaBackend path parentid getOrElse "???") + "/" + getName
    override def move(destination: FtpFile): Boolean = ???
    override def createInputStream(offset: Long): InputStream = ???
    override def delete(): Boolean = ???
    override def setLastModified(time: Long): Boolean = ???
    override def createOutputStream(offset: Long): OutputStream = ???
    override def mkdir(): Boolean = ???
  }

  case class VirtualRepoFile(override val parentid: Long, override val getName: String) extends RepoFile {
    override def isReadable: Boolean = false
    override def isFile: Boolean = false
    override def isDirectory: Boolean = false
    override def getLinkCount: Int = 0
    override def getSize: Long = 0L
    override def doesExist(): Boolean = false
    override def isRemovable: Boolean = false
    override def getLastModified: Long = now
    override def listFiles(): util.List[FtpFile] = util.Collections emptyList()
  }

  case class ActualRepoFile(treeEntry: TreeEntry) extends RepoFile {
    override def parentid: Long = if (treeEntry == rootEntry) treeEntry.id else treeEntry.parent

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
      seqAsJavaList (metaBackend children treeEntry.id map ActualRepoFile)
    }
  }
}
