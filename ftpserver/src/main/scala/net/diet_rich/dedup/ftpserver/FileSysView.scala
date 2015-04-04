package net.diet_rich.dedup.ftpserver

import org.apache.ftpserver.ftplet.{FtpFile, FileSystemView}

import net.diet_rich.dedup.core.{RepositoryReadWrite, Repository}
import net.diet_rich.dedup.core.meta.{TreeEntry, rootEntry}
import net.diet_rich.dedup.util.Logging

case class FileSysViewReadOnly(repository: Repository) extends FileSysView[Repository] {
  override def virtualFile(parentid: Long, name: String): VirtualRepoFile[Repository] = VirtualRepoFileReadOnly(repository, name, parentid)
  override def actualFile(treeEntry: TreeEntry): ActualRepoFile[Repository] = ActualRepoFileReadOnly(repository, treeEntry)
}

case class FileSysViewReadWrite(repository: RepositoryReadWrite, maxBytesToCache: Int) extends FileSysView[RepositoryReadWrite] {
  override def virtualFile(parentid: Long, name: String): VirtualRepoFile[RepositoryReadWrite] = VirtualRepoFileReadWrite(repository, name, parentid, maxBytesToCache)
  override def actualFile(treeEntry: TreeEntry): ActualRepoFile[RepositoryReadWrite] = ActualRepoFileReadWrite(repository, treeEntry, maxBytesToCache)
}

trait FileSysView[R <: Repository] extends FileSystemView with Logging {
  def repository: R
  def virtualFile(parentid: Long, name: String): VirtualRepoFile[R]
  def actualFile(treeEntry: TreeEntry): ActualRepoFile[R]

  log info "created ftp dedup file system view"

  protected val rootDirectory: ActualRepoFile[R] = actualFile(rootEntry)
  protected var workingDirectory: ActualRepoFile[R] = rootDirectory

  protected def resolvePath(path: String): Option[RepoFile[R]] = {
    def resolve(currentDir: ActualRepoFile[R], path: List[String]): Option[RepoFile[R]] = path match {
      case Nil          => Some(currentDir)
      case ""   :: tail => resolve(currentDir, tail)
      case "."  :: tail => resolve(currentDir, tail)
      case ".." :: tail => currentDir.parent flatMap (resolve(_, tail))
      case name :: Nil  => currentDir child name orElse Some(virtualFile(currentDir.treeEntry id, name))
      case name :: tail => currentDir child name flatMap (resolve(_, tail))
    }
    val (startDir, relativeDir) = if (path startsWith "/") (rootDirectory, path.substring(1)) else (workingDirectory, path)
    resolve(startDir, relativeDir split '/' toList)
  }

  override def changeWorkingDirectory(dir: String): Boolean = log.call(s"cd to $dir") {
    resolvePath(dir) match {
      case Some(repoFile: ActualRepoFile[R]) if repoFile.isDirectory => workingDirectory = repoFile; true
      case _ => false
    }
  }

  override def dispose(): Unit = log info "closed ftp dedup file system view"
  override def getFile(name: String): FtpFile = resolvePath(name) getOrElse (throw new IllegalArgumentException(s"could not resolve $workingDirectory/$name"))
  override def getHomeDirectory: FtpFile = rootDirectory
  override def getWorkingDirectory: FtpFile = workingDirectory
  override def isRandomAccessible: Boolean = false
}
