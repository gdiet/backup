// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.ftpserver

import net.diet_rich.dedup.core.FileSystem
import net.diet_rich.dedup.core.values.{TreeEntry, TreeEntryID}
import net.diet_rich.dedup.util.{CallLogging, Logging}
import org.apache.ftpserver.ftplet._
import scala.collection.JavaConversions
import java.io.IOException
import java.io.FileNotFoundException

class FileSysView(filesystem: FileSystem) extends FileSystemView with Logging with CallLogging {
  log info "creating ftp file system view"
  
  private val rootDirectory = IsRepoFile(FileSystem.ROOTID)
  private var workingDirectory: IsRepoFile = rootDirectory

  private def resolvePath(path: String): Option[RepoFile] = info(s"resolvePath($path)") {
    def resolve(currentDir: IsRepoFile, path: List[String]): Option[RepoFile] = path match {
      case Nil          => Some(currentDir)
      case ""   :: tail => resolve(currentDir, tail)
      case "."  :: tail => resolve(currentDir, tail)
      case ".." :: tail => currentDir.parent flatMap (resolve(_, tail))
      case name :: Nil  => currentDir child name orElse Some(MaybeRepoFile(currentDir.id, name))
      case name :: tail => currentDir child name flatMap (resolve(_, tail))
    }
    val (startDir, relativeDir) = if (path.startsWith("/")) (rootDirectory, path.substring(1)) else (workingDirectory, path)
    resolve(startDir, relativeDir split '/' toList)
  }
  
  override def changeWorkingDirectory(dir: String): Boolean = info(s"... cd $dir") {
    resolvePath(dir) match {
      case Some(repoFile) =>
        repoFile match {
          case repoFile: IsRepoFile => workingDirectory = repoFile; true
          case _ => false
        }
      case None => false
    }
  }

  override def dispose(): Unit = info("... dispose"){/**/} // TODO info("dispose")(System.exit(0)) ???

  override def getFile(name: String): FtpFile = info("... getting: " + name) {
    resolvePath(name) match {
      case Some(repoFile) => repoFile
      case None => throw new IllegalArgumentException("could not resolve path $name")
    }
  }

  override def getHomeDirectory(): RepoFile = info("... getHomeDirectory")(rootDirectory)

  override def getWorkingDirectory(): RepoFile = info("... getWorkingDirectory")(workingDirectory)

  // TODO random access can be easily implemented later on - see FtpFile.createInputStream and FtpFile.createOutputStream
  override def isRandomAccessible(): Boolean = info("... isRandomAccessible")(false)

  sealed trait RepoFile extends FtpFile

  case class MaybeRepoFile(parent: TreeEntryID, name: String) extends RepoFile with Logging with CallLogging {
    log.info(s"creating maybe repo file for id $parent/$name")

    override val toString = s"MaybeRepoFile($parent/$name)"

    def getAbsolutePath(): String = info(s"getAbsolutePath for $this") {
      filesystem.path(parent) match {
        case Some(path) => path.value + "/" + name
        case _ => info("WARN: getAbsolutePath - node $parent does not exist")("")
      }
    }
    def isFile(): Boolean = info(s"isFile for $this") { false }
    def isDirectory(): Boolean = info(s"isDirectory for $this") { false }
    def listFiles(): java.util.List[FtpFile] = info(s"listFiles for $this") { JavaConversions.seqAsJavaList(Seq()) }
    def isHidden(): Boolean = info(s"isHidden for $this") { false }
    def getSize(): Long = info(s"getSize for $this") { 0L }
    def getLastModified(): Long = info(s"getLastModified for $this") { 0L }
    def isReadable(): Boolean = info(s"isReadable for $this") { false }
    def doesExist(): Boolean = info(s"doesExist for $this") { false }
    def isWritable(): Boolean = info(s"isWritable for $this") { ??? }
    def getName(): String = info(s"getName for $this") { name }
    def getLinkCount(): Int = info(s"getLinkCount for $this") { 0 }
    def getOwnerName(): String = info(s"getOwnerName for $this") { "backup" }
    def getGroupName(): String = info(s"getGroupName for $this") { "dedup" }
    def createInputStream(offset: Long): java.io.InputStream = info(s"createInputStream with offset $offset for $this") { throw new FileNotFoundException }
    def createOutputStream(x$1: Long): java.io.OutputStream = info(s"createOutputStream for $this") { ??? }
    def delete(): Boolean = info(s"delete $this") { false }
    def isRemovable(): Boolean = info(s"isRemovable for $this") { false }
    def mkdir(): Boolean = info(s"mkdir for $this") { ??? }
    def move(target: org.apache.ftpserver.ftplet.FtpFile): Boolean = info(s"move for $this to $target") { ??? }
    def setLastModified(x$1: Long): Boolean = info(s"setLastModified for $this") { ??? }
  }

  case class IsRepoFile(id: TreeEntryID) extends RepoFile with Logging with CallLogging {
    log.info(s"creating repo file for id $id")

    override val toString = s"RepoFile($id)"

    def parent: Option[IsRepoFile] =
      filesystem.entry(id).map(entry => IsRepoFile(entry.parent))

    def getAbsolutePath(): String = info(s"getAbsolutePath for $this") {
      filesystem.path(id) match {
        case Some(path) => path.value
        case _ => info("WARN: getAbsolutePath - node $id does not exist")("")
      }
    }

    def isFile(): Boolean = info(s"isFile for $this") {
      filesystem.entry(id).flatMap(_.data).isDefined
    }

    def isDirectory(): Boolean = info(s"isDirectory for $this") {
      val entry = filesystem.entry(id)
      entry.isDefined && entry.flatMap(_.data).isEmpty
    }

    def child(name: String) =
      filesystem.firstChild(id, name).map(e => IsRepoFile(e.id))

    def children =
      filesystem.children(id).map(e => IsRepoFile(e.id))

    def listFiles(): java.util.List[FtpFile] = info(s"listFiles for $this") {
      JavaConversions.seqAsJavaList(children.toSeq)
    }

    def isHidden(): Boolean = info(s"isHidden for $this") { false }

    def getSize(): Long = info(s"getSize for $this") {
      ( for {
        treeEntry <- filesystem entry id
        dataid    <- treeEntry.data
        dataEntry <- filesystem dataEntry dataid
      } yield dataEntry.size.value) getOrElse 0L
    }

    def getLastModified(): Long = info(s"getLastModified for $this") {
      ( for {
        treeEntry <- filesystem entry id
        changed   <- treeEntry.changed
      } yield changed.value) getOrElse 0L
    }

    def isReadable(): Boolean = info(s"isReadable for $this") {
      filesystem.entry(id).isDefined
    }

    def doesExist(): Boolean = info(s"doesExist for $this") {
      filesystem.entry(id).isDefined
    }

    def isWritable(): Boolean = info(s"isWritable for $this") { false } // TODO implement write

    def getName(): String = info(s"getName for $this") {
      filesystem.entry(id).map(_.name).getOrElse("")
    }

    def getLinkCount(): Int = info(s"getLinkCount for $this") {
      if (doesExist) 1 else 0
    }

    def getOwnerName(): String = info(s"getOwnerName for $this") { "backup" }

    def getGroupName(): String = info(s"getGroupName for $this") { "dedup" }

    def createInputStream(offset: Long): java.io.InputStream = info(s"createInputStream with offset $offset for $this") {
      if (offset != 0) throw new IOException("not random accessible")
      filesystem.entry(id) match {
        case None => throw new FileNotFoundException
        case Some(TreeEntry(_, _, _, _, None, _)) => throw new IOException("directory, not a file")
        case Some(TreeEntry(_, _, _, _, Some(dataid), _)) =>
          filesystem.dataEntry(dataid).fold{
            throw new IOException(s"no data entry found for $dataid")
          }{ dataEntry =>
            filesystem.read(dataid)
            ???
          }
      }
    }

    def createOutputStream(x$1: Long): java.io.OutputStream = info(s"createOutputStream for $this") { ??? }

    def delete(): Boolean = info(s"delete $this") { filesystem.markDeleted(id) }

    def isRemovable(): Boolean = info(s"isRemovable for $this") { (??? == true) && (id != FileSystem.ROOTID) }

    def mkdir(): Boolean = info(s"mkdir for $this") { ??? }

    def move(target: org.apache.ftpserver.ftplet.FtpFile): Boolean = info(s"move for $this to $target") {
      target match {
        case target: MaybeRepoFile => ??? // filesystem.changePath(id, target.name, target.parent)
        case _ => false
      }
    }

    def setLastModified(x$1: Long): Boolean = info(s"setLastModified for $this") { ??? }
  }

}
