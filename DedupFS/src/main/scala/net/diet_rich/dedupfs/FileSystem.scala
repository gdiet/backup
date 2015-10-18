package net.diet_rich.dedupfs

import java.io.IOException

import net.diet_rich.common._
import net.diet_rich.dedupfs.metadata.TreeEntry, TreeEntry.RichPath

class FileSystem(val repository: Repository.Any) extends AutoCloseable { import repository._
  val maybeRepositoryReadWrite = repository match { case r: Repository => Some(r); case _ => None }
  val isReadOnly = maybeRepositoryReadWrite.isEmpty
  val root = new ActualFile(this, TreeEntry.rootPath, TreeEntry.root)

  // FIXME check where to implement - here or in the files
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
  def isDirectory: Boolean
  def isFile: Boolean
  def exists: Boolean
  def isWritable: Boolean
  def size: Long
  def lastModified: Long
}

class VirtualFile(fs: FileSystem, val path: String) extends DedupFile {
  override def name: String = (TreeEntry pathElements path).last
  override def mkDir(): Boolean = fs mkDir path
  override def exists: Boolean = false
  override def isDirectory: Boolean = false
  override def isFile: Boolean = false
  override def isWritable: Boolean = ! fs.isReadOnly
  override def size: Long = 0L
  override def lastModified: Long = 0L
}

class ActualFile(fs: FileSystem, val path: String, entry: TreeEntry) extends DedupFile {
  override def name: String = entry.name
  override def mkDir(): Boolean = false
  override def exists: Boolean = true
  override def isDirectory: Boolean = entry.data.nonEmpty
  override def isFile: Boolean = entry.data.isEmpty
  override def isWritable: Boolean = !fs.isReadOnly && !(entry == TreeEntry.root)
  override def size: Long = entry.data flatMap fs.repository.metaBackend.sizeOf getOrElse 0L
  override def lastModified: Long = entry.changed getOrElse 0L
}
