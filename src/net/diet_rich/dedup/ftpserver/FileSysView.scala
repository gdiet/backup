// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.ftpserver

import Helpers._
import net.diet_rich.dedup.database._
import net.diet_rich.dedup.repository.Repository
import org.apache.ftpserver.ftplet._
import scala.collection.JavaConversions

object Helpers {
  def log(msg: String) = System.err.println(msg)
  def logAnd[Result](msg: String)(andThen: => Result): Result = {
    val result = andThen
    log(msg + " -> " + result);
    result
  }
}

class FileSysView(repository: Repository) extends FileSystemView {
  var workingDirectory = getHomeDirectory
  def changeWorkingDirectory(dir: String): Boolean = logAnd(f"cd $dir") {
    if (!dir.contains("/") && !dir.contains(".")) {
      workingDirectory.listFilesScala.find(_.getName == dir) match {
        case Some(newDir) =>
          workingDirectory = newDir
          true
        case None =>
          false
      }
    } else if (dir == "..") {
      workingDirectory.getParent match {
        case Some(newDir) =>
          workingDirectory = newDir
          true
        case None =>
          false
      }
    } else if (dir == "/") {
      workingDirectory = getHomeDirectory
      true
    } else {
      System.err.println(f"cd $dir");
      Thread.sleep(500)
      ???
    }
  }
  def dispose(): Unit = logAnd("dispose")(System.exit(0))
  def getFile(name: String): FtpFile = logAnd("getting: " + name) {
    if (name == "./")
      workingDirectory
    else
      ???
  }
  def getHomeDirectory(): RepoFile = logAnd("getHomeDirectory")(new RepoFile(repository, TreeDB.ROOTID))
  def getWorkingDirectory(): RepoFile = logAnd("getWorkingDirectory")(workingDirectory)
  def isRandomAccessible(): Boolean = ???
}

class RepoFile(repository: Repository, id: TreeEntryID) extends FtpFile {
  log(f"creating repo file for id $id")
  
  override val toString = f"RepoFile($id)"

  def getParent: Option[RepoFile] =
    repository.fs.entry(id).flatMap(_.parentOption.map(new RepoFile(repository, _)))
  
  override def getAbsolutePath(): String = logAnd("getAbsolutePath for " + id) {
    repository.fs.path(id) match {
      case Some(path) => path.value
      case _ => logAnd("WARN: getAbsolutePath - node $id does not exist")("")
    }
  }

  override def isFile(): Boolean = logAnd("isFile for " + id) {
    repository.fs.fullDataInformation(id).isDefined
  }

  override def isDirectory(): Boolean = logAnd("isDirectory for " + id) {
    repository.fs.entry(id).isDefined && (!isFile)
  }

  def listFilesScala =
    repository.fs.children(id).map(e => new RepoFile(repository, e.id))
  
  override def listFiles(): java.util.List[FtpFile] = logAnd("listFiles for " + id) {
    JavaConversions.seqAsJavaList(listFilesScala.toSeq)
  }

  override def isHidden(): Boolean = logAnd("isHidden for " + id) { false }
  
  override def getSize(): Long = logAnd("getSize for " + id) {
    repository.fs.fullDataInformation(id).map(_.size.value).getOrElse(0L)
  }

  override def getLastModified(): Long = logAnd("getLastModified for " + id) {
    repository.fs.entry(id).map(_.time.value).getOrElse(0L)
  }
  
  override def isReadable(): Boolean = logAnd("isReadable for " + id) {
    repository.fs.entry(id).isDefined
  }

  override def doesExist(): Boolean = logAnd("doesExist for " + id) {
    repository.fs.entry(id).isDefined
  }
  
  override def isWritable(): Boolean = logAnd("isWritable for " + id) { false } // FIXME write

  override def getName(): String = logAnd("getLastModified for " + id) {
    repository.fs.entry(id).map(_.name).getOrElse("")
  }

  override def getLinkCount(): Int = logAnd("getLinkCount for " + id) {
    if (doesExist) 1 else 0
  }

  override def getOwnerName(): String = logAnd(f"getOwnerName for $id") { "backup" }

  override def getGroupName(): String = logAnd(f"getGroupName for $id") { "dedup" }
  
  override def createInputStream(x$1: Long): java.io.InputStream = ???
  override def createOutputStream(x$1: Long): java.io.OutputStream = ???
  override def delete(): Boolean = ???
  override def isRemovable(): Boolean = ???
  override def mkdir(): Boolean = ???
  override def move(x$1: org.apache.ftpserver.ftplet.FtpFile): Boolean = ???
  override def setLastModified(x$1: Long): Boolean = ???
  
}
