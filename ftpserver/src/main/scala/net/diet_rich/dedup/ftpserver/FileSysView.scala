// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.ftpserver

import Helpers._
import net.diet_rich.dedup.database._
import net.diet_rich.dedup.repository.Repository
import org.apache.ftpserver.ftplet._
import scala.collection.JavaConversions
import java.io.IOException
import java.io.FileNotFoundException

object Helpers {
  def log(msg: => String) = () // System.err.println("::: " + msg)
  def logAnd[Result](msg: => String)(andThen: => Result): Result = {
    val result = andThen
    log(msg + " -> " + result);
    result
  }
}

class FileSysView(repository: Repository) extends FileSystemView {
  log("... creating file system view")
  
  private val rootDirectory = IsRepoFile(repository, repository.fs.ROOTID)
  var workingDirectory: IsRepoFile = rootDirectory

  def resolvePath(path: String): Option[RepoFile] =  logAnd("... resolvePath: " + path) {
    def cdTo(currentDir: IsRepoFile, path: List[String]): Option[RepoFile] = path match {
      case Nil => Some(currentDir)
      case ""  :: tail => cdTo(currentDir, tail)
      case "." :: tail => cdTo(currentDir, tail)
      case "..":: tail => currentDir.getParent.flatMap(cdTo(_, tail))
      case e   :: Nil  => currentDir.listFilesScala.find(_.getName == e).orElse(Some(MaybeRepoFile(repository, currentDir.id, e)))
      case e   :: tail => currentDir.listFilesScala.find(_.getName == e).flatMap(cdTo(_, tail))
    }
    val (startDir, relativeDir) =
      if (path.startsWith("/")) (rootDirectory, path.substring(1)) else (workingDirectory, path)
    cdTo(startDir, relativeDir.split('/').toList)
  }
  
  def changeWorkingDirectory(dir: String): Boolean = logAnd(s"... cd $dir") {
    resolvePath(dir) match {
      case Some(repoFile) =>
        repoFile match {
          case repoFile: IsRepoFile => workingDirectory = repoFile; true
          case _ => false
        }
      case None => false
    }
  }
  
  def dispose(): Unit = log("... dispose") // logAnd("dispose")(System.exit(0))
  
  def getFile(name: String): FtpFile = logAnd("... getting: " + name) {
    resolvePath(name) match {
      case Some(repoFile) => repoFile
      case None => throw new IllegalArgumentException("could not resolve path $name")
    }
  }
  
  def getHomeDirectory(): RepoFile = logAnd("... getHomeDirectory")(rootDirectory)

  def getWorkingDirectory(): RepoFile = logAnd("... getWorkingDirectory")(workingDirectory)

  // TODO 12 random access can be easily implemented later on - see FtpFile.createInputStream and FtpFile.createOutputStream
  def isRandomAccessible(): Boolean = logAnd("... isRandomAccessible")(false)
}

trait RepoFile extends FtpFile

case class MaybeRepoFile(repository: Repository, parent: TreeEntryID, name: String) extends RepoFile {
  log(s"creating maybe repo file for id $parent/$name")
  
  override val toString = s"MaybeRepoFile($parent/$name)"

  def getAbsolutePath(): String = logAnd(s"getAbsolutePath for $this") {
    repository.fs.path(parent) match {
      case Some(path) => path.value + "/" + name
      case _ => logAnd("WARN: getAbsolutePath - node $parent does not exist")("")
    }
  }
  def isFile(): Boolean = logAnd(s"isFile for $this") { false }
  def isDirectory(): Boolean = logAnd(s"isDirectory for $this") { false }
  def listFiles(): java.util.List[FtpFile] = logAnd(s"listFiles for $this") { JavaConversions.seqAsJavaList(Seq()) }
  def isHidden(): Boolean = logAnd(s"isHidden for $this") { false }
  def getSize(): Long = logAnd(s"getSize for $this") { 0L }
  def getLastModified(): Long = logAnd(s"getLastModified for $this") { 0L }
  def isReadable(): Boolean = logAnd(s"isReadable for $this") { false }
  def doesExist(): Boolean = logAnd(s"doesExist for $this") { false }
  def isWritable(): Boolean = logAnd(s"isWritable for $this") { (!repository.readonly) }
  def getName(): String = logAnd(s"getName for $this") { name }
  def getLinkCount(): Int = logAnd(s"getLinkCount for $this") { 0 }
  def getOwnerName(): String = logAnd(s"getOwnerName for $this") { "backup" }
  def getGroupName(): String = logAnd(s"getGroupName for $this") { "dedup" }
  def createInputStream(offset: Long): java.io.InputStream = logAnd(s"createInputStream with offset $offset for $this") { throw new FileNotFoundException }
  def createOutputStream(x$1: Long): java.io.OutputStream = logAnd(s"createOutputStream for $this") { ??? }
  def delete(): Boolean = logAnd(s"delete $this") { false }
  def isRemovable(): Boolean = logAnd(s"isRemovable for $this") { false }
  def mkdir(): Boolean = logAnd(s"mkdir for $this") { ??? }
  def move(target: org.apache.ftpserver.ftplet.FtpFile): Boolean = logAnd(s"move for $this to $target") { ??? }
  def setLastModified(x$1: Long): Boolean = logAnd(s"setLastModified for $this") { ??? }
}

case class IsRepoFile(repository: Repository, id: TreeEntryID) extends RepoFile {
  log(s"creating repo file for id $id")
  
  override val toString = s"RepoFile($id)"

  def getParent: Option[IsRepoFile] =
    repository.fs.entry(id).map(entry => IsRepoFile(repository, entry.parent))
  
  def getAbsolutePath(): String = logAnd(s"getAbsolutePath for $this") {
    repository.fs.path(id) match {
      case Some(path) => path.value
      case _ => logAnd("WARN: getAbsolutePath - node $id does not exist")("")
    }
  }

  def isFile(): Boolean = logAnd(s"isFile for $this") {
    repository.fs.fullDataInformation(id).isDefined
  }

  def isDirectory(): Boolean = logAnd(s"isDirectory for $this") {
    repository.fs.entry(id).isDefined && (!isFile)
  }

  def listFilesScala =
    repository.fs.children(id).map(e => IsRepoFile(repository, e.id))
  
  def listFiles(): java.util.List[FtpFile] = logAnd(s"listFiles for $this") {
    JavaConversions.seqAsJavaList(listFilesScala.toSeq)
  }

  def isHidden(): Boolean = logAnd(s"isHidden for $this") { false }
  
  def getSize(): Long = logAnd(s"getSize for $this") {
    repository.fs.fullDataInformation(id).map(_.size.value).getOrElse(0L)
  }

  def getLastModified(): Long = logAnd(s"getLastModified for $this") {
    repository.fs.entry(id).map(_.time.value).getOrElse(0L)
  }
  
  def isReadable(): Boolean = logAnd(s"isReadable for $this") {
    repository.fs.entry(id).isDefined
  }

  def doesExist(): Boolean = logAnd(s"doesExist for $this") {
    repository.fs.entry(id).isDefined
  }
  
  def isWritable(): Boolean = logAnd(s"isWritable for $this") { false } // TODO implement write

  def getName(): String = logAnd(s"getName for $this") {
    repository.fs.entry(id).map(_.name).getOrElse("")
  }

  def getLinkCount(): Int = logAnd(s"getLinkCount for $this") {
    if (doesExist) 1 else 0
  }

  def getOwnerName(): String = logAnd(s"getOwnerName for $this") { "backup" }

  def getGroupName(): String = logAnd(s"getGroupName for $this") { "dedup" }
  
  def createInputStream(offset: Long): java.io.InputStream = logAnd(s"createInputStream with offset $offset for $this") {
    if (offset != 0) throw new IOException("not random accessible")
    repository.fs.entry(id) match {
      case None => throw new FileNotFoundException
      case Some(TreeEntry(_, _, _, _, _, _, None)) => throw new IOException("directory, not a file")
      case Some(TreeEntry(_, _, _, _, _, _, Some(dataid))) =>
        val method = repository.fs.dataEntry(dataid).method
        net.diet_rich.util.io.sourceAsInputStream(repository.fs.read(dataid, method))
    }
  }
  
  def createOutputStream(x$1: Long): java.io.OutputStream = logAnd(s"createOutputStream for $this") { ??? }
  
  def delete(): Boolean = logAnd(s"delete $this") { repository.fs.markDeleted(id) }
  
  def isRemovable(): Boolean = logAnd(s"isRemovable for $this") { (!repository.readonly) && (id != repository.fs.ROOTID) }
  
  def mkdir(): Boolean = logAnd(s"mkdir for $this") { ??? }
  
  def move(target: org.apache.ftpserver.ftplet.FtpFile): Boolean = logAnd(s"move for $this to $target") {
    target match {
      case target: MaybeRepoFile => repository.fs.changePath(id, target.name, target.parent)
      case _ => false
    }
  }
  
  def setLastModified(x$1: Long): Boolean = logAnd(s"setLastModified for $this") { ??? }
  
}
