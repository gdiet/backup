package net.diet_rich.dedup.ftpserver

import net.diet_rich.dedup.util.Logging
import org.apache.ftpserver.ftplet.{FtpFile, FileSystemView}

import net.diet_rich.dedup.core.Repository
import net.diet_rich.dedup.core.meta.rootEntry

case class FileSysView[R <: Repository](repository: R, maxBytesToCache: Int) extends FileSystemView with Logging {
  protected val repoFiles = new RepoFiles(repository, maxBytesToCache)
  import repoFiles._

  log info "created ftp dedup file system view"

  protected val rootDirectory: ActualRepoFile = ActualRepoFile(rootEntry)
  protected var workingDirectory: ActualRepoFile = rootDirectory

  protected def resolvePath(path: String): Option[RepoFile] = {
    def resolve(currentDir: ActualRepoFile, path: List[String]): Option[RepoFile] = path match {
      case Nil          => Some(currentDir)
      case ""   :: tail => resolve(currentDir, tail)
      case "."  :: tail => resolve(currentDir, tail)
      case ".." :: tail => currentDir.parent flatMap (resolve(_, tail))
      case name :: Nil  => currentDir child name orElse Some(VirtualRepoFile(currentDir.treeEntry id, name))
      case name :: tail => currentDir child name flatMap (resolve(_, tail))
    }
    val (startDir, relativeDir) = if (path startsWith "/") (rootDirectory, path.substring(1)) else (workingDirectory, path)
    resolve(startDir, relativeDir split '/' toList)
  }

  override def changeWorkingDirectory(dir: String): Boolean =
    resolvePath(dir) match {
      case Some(repoFile: ActualRepoFile) if repoFile.isDirectory => workingDirectory = repoFile; true
      case _ => false
    }

  override def dispose(): Unit = log info "closed ftp dedup file system view"
  override def getFile(name: String): FtpFile = resolvePath(name) getOrElse (throw new IllegalArgumentException(s"could not resolve $workingDirectory/$name"))
  override def getHomeDirectory: FtpFile = rootDirectory
  override def getWorkingDirectory: FtpFile = workingDirectory
  override def isRandomAccessible: Boolean = false // TODO random access - see FtpFile.createInputStream and FtpFile.createOutputStream
}
