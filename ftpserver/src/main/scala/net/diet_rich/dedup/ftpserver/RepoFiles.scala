package net.diet_rich.dedup.ftpserver

import java.io._
import java.util

import scala.collection.JavaConversions.seqAsJavaList

import org.apache.ftpserver.ftplet.FtpFile

import net.diet_rich.dedup.core.{Source, Repository, RepositoryReadOnly}
import net.diet_rich.dedup.core.meta.{rootEntry, TreeEntry}
import net.diet_rich.dedup.util.{Memory, Logging, now}

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
    override def mkdir(): Boolean = log.call(s"mkdir $getName in $parentid") {
      isWritable && { metaBackend.create(parentid, getName); true }
    }
  }

  object ActualRepoFile {
    def apply(treeEntry: TreeEntry) =
      writeAccess.fold(new ActualRepoFileReadOnly(treeEntry))(new ActualRepoFileReadWrite(treeEntry, _))
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

  class ActualRepoFileReadWrite(treeEntry: TreeEntry, repository: Repository) extends ActualRepoFileReadOnly(treeEntry) {
    override def move(destination: FtpFile): Boolean = log.call(s"move: $treeEntry to $destination") {
      treeEntry != rootEntry && (destination match {
        case VirtualRepoFile(parentid, newName) => metaBackend inTransaction {
          if (metaBackend children (parentid, newName) isEmpty)
            metaBackend change (treeEntry id, parentid, newName, treeEntry changed, treeEntry data) isDefined
          else false
        }
        case _ => false
      })
    }
    override def setLastModified(time: Long): Boolean = log.call(s"setLastModified: $treeEntry") {
      treeEntry != rootEntry && metaBackend.change(treeEntry id, parentid, treeEntry name, Some(time), treeEntry data).isDefined
    }
    override def delete(): Boolean = log.call(s"delete: $treeEntry") {
      treeEntry != rootEntry && metaBackend.markDeleted(treeEntry.id)
    }
    override def createOutputStream(offset: Long): OutputStream = log.call(s"createOutputStream: $treeEntry") {
      if (offset != 0) throw new IOException("not random accessible")
      if (isDirectory) throw new IOException("directory - can't write data")
      // TODO use FilterOutputStream to divert data to temp file if memory runs low
      new ByteArrayOutputStream() {
        override def write(i: Int): Unit = write(Array(i.toByte), 0, 1)
        override def write(data: Array[Byte]): Unit = write(data, 0, data.length)
        override def write(data: Array[Byte], offset: Int, length: Int): Unit = {
          Memory.reserve(length * 2) match {
            case _: Memory.Reserved =>
              super.write(data, offset, length)
            case _: Memory.NotAvailable =>
              Memory.free(count * 2)
              throw new IOException("out of memory, can't buffer")
          }
        }
        override def close(): Unit = try {
          val source = Source.from(new ByteArrayInputStream(buf, 0, count), count)
          val dataid = repository.storeLogic.dataidFor(source)
          // FIXME utility to clean up orphan data entries (and orphan byte store entries)
          if (metaBackend.change(treeEntry id, treeEntry parent, treeEntry name, Some(now), Some(dataid)).isEmpty)
            throw new IOException("could not write data")
        } finally {
          // FIXME this might free the memory a second time, see above...
          Memory.free(count * 2)
        }
      }
    }
  }

}
