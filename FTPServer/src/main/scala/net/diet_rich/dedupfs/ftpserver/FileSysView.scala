package net.diet_rich.dedupfs.ftpserver

import java.io.{InputStream, OutputStream}
import java.util

import org.apache.ftpserver.ftplet.{FtpFile, FileSystemView}

import net.diet_rich.common.Logging
import net.diet_rich.dedupfs.{DedupFile, FileSystem}

// FIXME in file list, display ".." and "." as usual (".." not in root)
class FileSysView(fs: FileSystem) extends FileSystemView with Logging {
  protected var workingDirectory: FileSysFile = getHomeDirectory
  override def isRandomAccessible: Boolean = false
  override def dispose(): Unit = log info "disposed ftp dedup file system view"
  override def getHomeDirectory: FileSysFile = new FileSysFile(fs.root)
  override def getWorkingDirectory: FtpFile = workingDirectory
  override def changeWorkingDirectory(dir: String): Boolean = resolvePath(dir) match {
    case Some(file) if file.isDirectory => workingDirectory = new FileSysFile(file); true
    case _ => false
  }
  override def getFile(path: String): FtpFile = resolvePath(path) map (new FileSysFile(_)) getOrElse {
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

class FileSysFile(private[ftpserver] val file: DedupFile) extends FtpFile {
  override def doesExist(): Boolean = file.exists
  override def isFile: Boolean = file.isFile
  override def isDirectory: Boolean = file.isDirectory
  override def isHidden: Boolean = false
  override def isReadable: Boolean = true
  override def isWritable: Boolean = file.isWritable
  override def getLinkCount: Int = 1
  override def getOwnerName: String = "user"
  override def getGroupName: String = "user"
  override def getName: String = file.name
  override def isRemovable: Boolean = file.isWritable
  override def getSize: Long = file.size
  override def getLastModified: Long = file.lastModified
  override def getAbsolutePath: String = file.path
  override def mkdir(): Boolean = file mkDir()
  override def move(destination: FtpFile): Boolean = ???
  override def createInputStream(offset: Long): InputStream = ???
  override def listFiles(): util.List[FtpFile] = ???
  override def delete(): Boolean = ???
  override def setLastModified(time: Long): Boolean = ???
  override def createOutputStream(offset: Long): OutputStream = ???
}
