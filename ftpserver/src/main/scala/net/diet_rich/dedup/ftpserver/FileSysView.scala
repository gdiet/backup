// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.ftpserver

import java.io.IOException
import java.io.FileNotFoundException
import scala.collection.JavaConversions

import org.apache.ftpserver.ftplet._

import net.diet_rich.dedup.core.FileSystem
import net.diet_rich.dedup.core.values.{Bytes, Time, TreeEntry, TreeEntryID}
import net.diet_rich.dedup.util.{CallLogging, Logging}

class FileSysView(filesystem: FileSystem, writeEnabled: Boolean) extends FileSystemView with Logging with CallLogging {
  log info "creating ftp file system view"
  
  private val rootDirectory = IsRepoFile(FileSystem.ROOTID)
  private var workingDirectory: IsRepoFile = rootDirectory

  private def resolvePath(path: String): Option[RepoFile] = debug(s"... resolvePath($path)") {
    def resolve(currentDir: IsRepoFile, path: List[String]): Option[RepoFile] = path match {
      case Nil          => Some(currentDir)
      case ""   :: tail => resolve(currentDir, tail)
      case "."  :: tail => resolve(currentDir, tail)
      case ".." :: tail => currentDir.parent flatMap (resolve(_, tail))
      // this case is needed for creating new files
      case name :: Nil  => currentDir child name orElse Some(MaybeRepoFile(currentDir.id, name))
      case name :: tail => currentDir child name flatMap (resolve(_, tail))
    }
    val (startDir, relativeDir) = if (path.startsWith("/")) (rootDirectory, path.substring(1)) else (workingDirectory, path)
    resolve(startDir, relativeDir split '/' toList)
  }
  
  override def changeWorkingDirectory(dir: String): Boolean = info(s"... cd($dir)") {
    resolvePath(dir) match {
      case Some(repoFile: IsRepoFile) => workingDirectory = repoFile; true
      case _ => false
    }
  }

  override def dispose(): Unit = log info "... dispose" // TODO info("dispose")(System.exit(0)) ???

  override def getFile(name: String): FtpFile = info(s"... getFile($name)") {
    resolvePath(name) match {
      case Some(repoFile) => repoFile
      case None => throw new IllegalArgumentException(s"could not resolve $workingDirectory / $name")
    }
  }

  override def getHomeDirectory: RepoFile = info("... getHomeDirectory")(rootDirectory)

  override def getWorkingDirectory: RepoFile = info("... getWorkingDirectory")(workingDirectory)

  // TODO random access can be easily implemented later on - see FtpFile.createInputStream and FtpFile.createOutputStream
  override def isRandomAccessible: Boolean = info("... isRandomAccessible")(false)

  // ******** end of implementation of FileSystemView interface ********

  sealed trait RepoFile extends FtpFile with Logging with CallLogging {
    override final def getOwnerName: String = debug(s"getOwnerName for $this") { "backup" }
    override final def getGroupName: String = debug(s"getGroupName for $this") { "dedup" }
    override final def isHidden: Boolean = debug(s"isHidden for $this") { false }
    override final def isWritable: Boolean = debug(s"isWritable for $this") { writeEnabled }
  }

  /** A not-yet-existing ftp repo file used when creating new files or directories. */
  case class MaybeRepoFile(parent: TreeEntryID, name: String) extends RepoFile {
    override val toString = s"MaybeRepoFile($parent/$name)"
    log debug s"creating $this"

    override def getAbsolutePath: String = debug(s"getAbsolutePath for $this") {
      filesystem.path(parent) match {
        case Some(path) => path.value + "/" + name
        case _ => debug("WARN: getAbsolutePath - node $parent does not exist")("")
      }
    }
    override def isFile: Boolean = debug(s"isFile for $this") { false }
    override def isDirectory: Boolean = debug(s"isDirectory for $this") { false }
    override def listFiles: java.util.List[FtpFile] = debug(s"listFiles for $this") { java.util.Collections emptyList() }
    override def getSize: Long = debug(s"getSize for $this") { 0L }
    override def getLastModified: Long = debug(s"getLastModified for $this") { 0L }
    override def isReadable: Boolean = debug(s"isReadable for $this") { false }
    override def doesExist: Boolean = debug(s"doesExist for $this") { false }
    override def getName: String = debug(s"getName for $this") { name }
    override def getLinkCount: Int = debug(s"getLinkCount for $this") { 0 }
    override def createInputStream(offset: Long): java.io.InputStream = debug(s"createInputStream with offset $offset for $this") { throw new FileNotFoundException }
    override def createOutputStream(x$1: Long): java.io.OutputStream = debug(s"createOutputStream for $this") { ??? } // FIXME implement
    override def delete(): Boolean = debug(s"delete $this") { false }
    override def isRemovable: Boolean = debug(s"isRemovable for $this") { false }
    override def mkdir(): Boolean = debug(s"mkdir for $this") { filesystem.createUnchecked(parent, name); true }
    override def move(target: org.apache.ftpserver.ftplet.FtpFile): Boolean = debug(s"move for $this to $target") { false }
    override def setLastModified(time: Long): Boolean = debug(s"setLastModified for $this") { false }
  }

  case class IsRepoFile(id: TreeEntryID) extends RepoFile {
    log debug s"creating $this"

    def child(name: String) = filesystem.firstChild(id, name).map(e => IsRepoFile(e.id))
    def parent: Option[IsRepoFile] = filesystem entry id map (entry => IsRepoFile(entry.parent))

    private def entry: Option[TreeEntry] = filesystem entry id

    override def getAbsolutePath: String = debug(s"getAbsolutePath for $this") { filesystem path id map (_.value) getOrElse { log.warn(s"getAbsolutePath for $id failed"); "" } }
    override def isFile: Boolean = debug(s"isFile for $this") { filesystem.dataid(id).isDefined }
    override def isDirectory: Boolean = debug(s"isDirectory for $this") { entry exists (_.data isEmpty) }
    override def listFiles(): java.util.List[FtpFile] = debug(s"listFiles for $this") { JavaConversions seqAsJavaList (filesystem children id map (e => IsRepoFile(e.id))) }
    override def getSize: Long = debug(s"getSize for $this") { filesystem dataEntry id map (_.size value) getOrElse 0L }
    override def getLastModified: Long = debug(s"getLastModified for $this") { entry flatMap (_.changed) map (_.value) getOrElse 0L }
    override def isReadable: Boolean = debug(s"isReadable for $this") { doesExist() }
    override def doesExist(): Boolean = debug(s"doesExist for $this") { entry.isDefined }
    override def getName: String = debug(s"getName for $this") { entry map (_.name) getOrElse "" }
    override def getLinkCount: Int = debug(s"getLinkCount for $this") { if (doesExist()) 1 else 0 }
    override def delete(): Boolean = debug(s"delete $this") { filesystem markDeleted id }
    override def isRemovable: Boolean = debug(s"isRemovable for $this") { writeEnabled && (id != FileSystem.ROOTID) }
    override def mkdir(): Boolean = debug(s"mkdir for $this") { false }
    override def setLastModified(newTime: Long): Boolean = debug(s"setLastModified for $this") { entry map (_ copy(changed = Some(Time(newTime)))) flatMap (filesystem change _) isDefined; }

    override def move(target: FtpFile): Boolean = debug(s"move for $this to $target") {
      target match {
        case target: MaybeRepoFile => filesystem inTransaction {
            if (filesystem.children(target.parent, target.name).isEmpty) {
              entry map (_ copy(parent = target.parent, name = target.name)) flatMap (filesystem change _) isDefined;
            } else false
          }
        case _ => false
      }
    }

    override def createInputStream(offset: Long): java.io.InputStream = debug(s"createInputStream with offset $offset for $this") {
      if (offset != 0) throw new IOException("not random accessible")
      entry match {
        case None => throw new FileNotFoundException(s"no entry in file system for $this")
        case Some(TreeEntry(_, _, _, _, None, _)) => throw new IOException(s"$this is a directory, not a file")
        case Some(TreeEntry(_, _, _, _, Some(dataid), _)) =>
          filesystem.dataEntry(dataid).fold {
            throw new IOException(s"no data entry found for $dataid in $this")
          }{ dataEntry =>
            filesystem read dataid asInputStream;
          }
      }
    }

    override def createOutputStream(offset: Long): java.io.OutputStream = debug(s"createOutputStream for $this") {
      if (offset != 0) throw new IOException("not random accessible")
      ???
    }
  }
}
