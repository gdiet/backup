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
  def log(msg: String) = System.err.println(msg)
  def logAnd[Result](msg: String)(andThen: => Result): Result = {
    val result = andThen
    log(msg + " -> " + result);
    result
  }
}

class FileSysView(repository: Repository) extends FileSystemView {
  log("creating file system view")
  
  var workingDirectory = getHomeDirectory

  def resolvePath(path: String): Option[RepoFile] = {
    def cdTo(currentDir: RepoFile, path: Seq[String]): Option[RepoFile] = {
      System.err.println("---- " + path + "  -> " + (if (path.isEmpty) "xxx" else path.head))
      path.toList match { // toList needed as workaround for https://issues.scala-lang.org/browse/SI-6996
        case Nil => Some(currentDir)
        case ""  :: tail => cdTo(currentDir, tail)
        case "." :: tail => cdTo(currentDir, tail)
        case "..":: tail => currentDir.getParent.flatMap(cdTo(_, tail))
        case e   :: tail => currentDir.listFilesScala.find(_.getName == e).flatMap(cdTo(_, tail))
      }
    }
    val (startDir, relativeDir) =
      if (path.startsWith("/")) (getHomeDirectory, path.substring(1)) else (workingDirectory, path)
    cdTo(startDir, relativeDir.split('/').toSeq)
  }
  
  def changeWorkingDirectory(dir: String): Boolean = logAnd(f"cd $dir") {
    resolvePath(dir) match {
      case None => false
      case Some(repoFile) => workingDirectory = repoFile; true
    }
  }
  
  def dispose(): Unit = logAnd("dispose")(System.exit(0))
  
  def getFile(name: String): FtpFile = logAnd("getting: " + name) {
    resolvePath(name) match {
      case Some(repoFile) => repoFile
      case None => throw new IllegalArgumentException("could not resolve path $name")
    }
  }
  
  def getHomeDirectory(): RepoFile = logAnd("getHomeDirectory")(new RepoFile(repository, TreeDB.ROOTID))

  def getWorkingDirectory(): RepoFile = logAnd("getWorkingDirectory")(workingDirectory)

  // TODO random access can be easily implemented later on - see FtpFile.createInputStream and FtpFile.createOutputStream
  def isRandomAccessible(): Boolean = logAnd("getWorkingDirectory")(false)
}

class RepoFile(repository: Repository, id: TreeEntryID) extends FtpFile {
  log(f"creating repo file for id $id")
  
  override val toString = f"RepoFile($id)"

  def getParent: Option[RepoFile] =
    repository.fs.entry(id).flatMap(_.parent.map(new RepoFile(repository, _)))
  
  def getAbsolutePath(): String = logAnd("getAbsolutePath for " + id) {
    repository.fs.path(id) match {
      case Some(path) => path.value
      case _ => logAnd("WARN: getAbsolutePath - node $id does not exist")("")
    }
  }

  def isFile(): Boolean = logAnd("isFile for " + id) {
    repository.fs.fullDataInformation(id).isDefined
  }

  def isDirectory(): Boolean = logAnd("isDirectory for " + id) {
    repository.fs.entry(id).isDefined && (!isFile)
  }

  def listFilesScala =
    repository.fs.children(id).map(e => new RepoFile(repository, e.id))
  
  def listFiles(): java.util.List[FtpFile] = logAnd("listFiles for " + id) {
    JavaConversions.seqAsJavaList(listFilesScala.toSeq)
  }

  def isHidden(): Boolean = logAnd("isHidden for " + id) { false }
  
  def getSize(): Long = logAnd("getSize for " + id) {
    repository.fs.fullDataInformation(id).map(_.size.value).getOrElse(0L)
  }

  def getLastModified(): Long = logAnd("getLastModified for " + id) {
    repository.fs.entry(id).map(_.time.value).getOrElse(0L)
  }
  
  def isReadable(): Boolean = logAnd("isReadable for " + id) {
    repository.fs.entry(id).isDefined
  }

  def doesExist(): Boolean = logAnd("doesExist for " + id) {
    repository.fs.entry(id).isDefined
  }
  
  def isWritable(): Boolean = logAnd("isWritable for " + id) { false } // TODO implement write

  def getName(): String = logAnd("getLastModified for " + id) {
    repository.fs.entry(id).map(_.name).getOrElse("")
  }

  def getLinkCount(): Int = logAnd("getLinkCount for " + id) {
    if (doesExist) 1 else 0
  }

  def getOwnerName(): String = logAnd(f"getOwnerName for $id") { "backup" }

  def getGroupName(): String = logAnd(f"getGroupName for $id") { "dedup" }
  
  def createInputStream(offset: Long): java.io.InputStream = logAnd(f"createInputStream with offset $offset") {
    if (offset != 0) throw new IOException("not random accessible")
    repository.fs.entry(id) match {
      case None => throw new FileNotFoundException
      case Some(TreeEntry(_, _, _, _, None)) => throw new IOException("directory, not a file")
      case Some(TreeEntry(_, _, _, _, Some(dataid))) =>
        val method = repository.fs.dataEntry(dataid).method
        net.diet_rich.util.io.sourceAsInputStream(repository.fs.read(dataid, method))
    }
  }
  
  def createOutputStream(x$1: Long): java.io.OutputStream = ???
  def delete(): Boolean = ???
  def isRemovable(): Boolean = ???
  def mkdir(): Boolean = ???
  def move(x$1: org.apache.ftpserver.ftplet.FtpFile): Boolean = ???
  def setLastModified(x$1: Long): Boolean = ???
  
}
