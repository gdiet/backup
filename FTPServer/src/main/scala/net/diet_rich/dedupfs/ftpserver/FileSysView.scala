package net.diet_rich.dedupfs.ftpserver

import java.io.{InputStream, OutputStream}
import java.util
import scala.collection.JavaConverters._

import org.apache.ftpserver.ftplet.{FtpFile, FileSystemView}

import net.diet_rich.common.Logging
import net.diet_rich.dedupfs.{DedupFile, FileSystem}

class FileSysView(fs: FileSystem) extends FileSystemView with Logging {
  protected var workingDirectory: FileSysFile = getHomeDirectory
  override def isRandomAccessible: Boolean = false
  override def dispose(): Unit = log info "disposed ftp dedup file system view"
  override def getHomeDirectory: FileSysFile = FileSysFile(fs.root)
  override def getWorkingDirectory: FtpFile = workingDirectory
  override def changeWorkingDirectory(dir: String): Boolean = resolvePath(dir) match {
    case Some(file) if file.isDirectory => workingDirectory = FileSysFile(file); true
    case _ => false
  }
  override def getFile(path: String): FtpFile = resolvePath(path) map (FileSysFile(_)) getOrElse {
    throw new IllegalArgumentException(s"could not resolve '$path' with working directory $workingDirectory")
  }

  protected def resolvePath(path: String): Option[DedupFile] = {
    def resolve(currentDir: DedupFile, path: List[String]): Option[DedupFile] = path match {
      case Nil          => Some(currentDir)
      case ""   :: tail => resolve(currentDir, tail)
      case "."  :: tail => resolve(currentDir, tail)
      case ".." :: tail => currentDir.parent flatMap (resolve(_, tail))
      case name :: Nil  => Some(currentDir child name)
      case name :: tail => resolve(currentDir child name, tail)
    }
    val (startDir, relativeDir) = if (path startsWith "/") (fs.root, path.substring(1)) else (workingDirectory.file, path)
    resolve(startDir, (relativeDir split '/').toList)
  }
}

object FileSysFile {
  def apply(file: DedupFile): FileSysFile = new FileSysFile(file, file.name)
}

class FileSysFile(private[ftpserver] val file: DedupFile, name: String) extends FtpFile with Logging { import log.call
  override def toString: String = s"FtpFile(name=$name,file=$file)"

  override def doesExist(): Boolean = call(s"${file.path} doesExist") { file.exists }
  override def isFile: Boolean = call(s"${file.path} isFile") { file.isFile }
  override def isDirectory: Boolean = call(s"${file.path} isDirectory") { file.isDirectory }
  override def isHidden: Boolean = call(s"${file.path} isHidden") { false }
  override def isReadable: Boolean = call(s"${file.path} isReadable") { true }
  override def isWritable: Boolean = call(s"${file.path} isWritable") { file.isWritable }
  override def getLinkCount: Int = call(s"${file.path} getLinkCount") { 1 }
  override def getOwnerName: String = call(s"${file.path} getOwnerName") { "user" }
  override def getGroupName: String = call(s"${file.path} getGroupName") { "user" }
  override def getName: String = call(s"${file.path} getName") { name }
  override def isRemovable: Boolean = call(s"${file.path} isRemovable") { file.isWritable }
  override def getSize: Long = call(s"${file.path} getSize") { file.size }
  override def getLastModified: Long = call(s"${file.path} getLastModified") { file.lastModified }
  override def getAbsolutePath: String = call(s"${file.path} getAbsolutePath") { file.path }
  override def mkdir(): Boolean = call(s"${file.path} mkdir") { file mkDir() }
  override def move(destination: FtpFile): Boolean = call(s"${file.path} move") { ??? }
  override def createInputStream(offset: Long): InputStream = call(s"${file.path} createInputStream") { ??? }
  override def listFiles(): util.List[FtpFile] = call(s"${file.path} listFiles") {
    (
      Seq(new FileSysFile(file, ".")) ++
      file.parent.map(new FileSysFile(_, "..")).toSeq ++
      file.children.map(FileSysFile(_): FtpFile)
    ).asJava
  }
  override def delete(): Boolean = call(s"${file.path} delete") { file.delete() }
  override def setLastModified(time: Long): Boolean = call(s"${file.path} setLastModified") { ??? }
  override def createOutputStream(offset: Long): OutputStream =call(s"${file.path} createOutputStream") { ??? }
}
