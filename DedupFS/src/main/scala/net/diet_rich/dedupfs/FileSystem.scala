package net.diet_rich.dedupfs

import java.io.IOException

import net.diet_rich.common._
import net.diet_rich.dedupfs.metadata.TreeEntry, TreeEntry.RichPath

class FileSystem(repository: Repository.Any) { import repository._
  val maybeRepositoryReadWrite = repository match { case r: Repository => Some(r); case _ => None }
  def write[T](f: Repository => T): T =
    maybeRepositoryReadWrite match {
      case None => throw new IOException("File system is read-only")
      case Some(repositoryReadWrite) => f(repositoryReadWrite)
    }

  def mkDir(path: String): Boolean = write { write =>
    val pathElements = TreeEntry pathElements path
    val (parentPath, Array(name)) = pathElements splitAt (pathElements.length - 1)
    metaBackend entry parentPath match {
      case Some(parent) => write.metaBackend create (parent.key, name, someNow, None); true
      case None => false
    }
  }

  def list(path: String): Seq[VirtualFile] =
    metaBackend entry path match {
      case None => Seq()
      case Some(parent) => metaBackend children parent.key map { entry => new VirtualFile(this, path / entry.name) }
    }

  def getFile(path: String) = new VirtualFile(this, path)
}

// FIXME also introduce ActualFile and RepoFile trait
class VirtualFile(repository: FileSystem, path: String) {
  def name: String = (TreeEntry pathElements path).last
  def mkDir(): Boolean = repository mkDir path
}
