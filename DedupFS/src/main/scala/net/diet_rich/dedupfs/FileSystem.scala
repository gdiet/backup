package net.diet_rich.dedupfs

import java.io.IOException

import net.diet_rich.common._
import net.diet_rich.dedupfs.metadata.TreeEntry, TreeEntry.RichPath

class FileSystem(repository: Repository.Any) extends AutoCloseable { import repository._
  val maybeRepositoryReadWrite = repository match { case r: Repository => Some(r); case _ => None }
  val isReadOnly = maybeRepositoryReadWrite.isEmpty
  val root = new ActualFile(this, TreeEntry.rootPath, TreeEntry.root)

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

  def list(path: String): Seq[DedupFile] =
    metaBackend entry path match {
      case None => Seq()
      case Some(parent) => metaBackend children parent.key map { entry => new ActualFile(this, path / entry.name, entry) }
    }

  def getFile(path: String): DedupFile =
    metaBackend entry path match {
      case None => new VirtualFile(this, path)
      case Some(entry) => new ActualFile(this, path, entry)
    }

  override def close(): Unit = repository.close()
}

trait DedupFile {
  def name: String
  def path: String
  def mkDir(): Boolean
  def parent: Option[DedupFile] = ???
  def child(name: String): DedupFile = ???
  def isDirectory: Boolean = ???
}

class VirtualFile(fs: FileSystem, val path: String) extends DedupFile {
  def name: String = (TreeEntry pathElements path).last
  def mkDir(): Boolean = fs mkDir path
}

class ActualFile(fs: FileSystem, val path: String, entry: TreeEntry) extends DedupFile {
  override def name: String = entry.name
  override def mkDir(): Boolean = false
}
