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

  def getFile(key: Long): Option[DedupFile] =
    metaBackend.entry(key) flatMap { parent =>
      metaBackend.path(parent.key) map (new ActualFile(this, _, parent))
    }

  override def close(): Unit = repository.close()
}

trait DedupFile {
  def name: String
  def path: String
  def mkDir(): Boolean
  def parent: Option[DedupFile]
  def child(name: String): DedupFile
  def children: Seq[DedupFile]
  def isDirectory: Boolean
  def isFile: Boolean
  def exists: Boolean
  def isWritable: Boolean
  def size: Long
  def lastModified: Long
}

// FIXME only allow virtual files for known parent ID
final class VirtualFile(fs: FileSystem, val path: String) extends DedupFile {
  override def toString: String = s"virtual:$path"
  override def name: String = (TreeEntry pathElements path).last
  override def mkDir(): Boolean = fs mkDir path
  override def exists: Boolean = false
  override def isDirectory: Boolean = false
  override def isFile: Boolean = false
  override def isWritable: Boolean = ! fs.isReadOnly
  override def size: Long = 0L
  override def lastModified: Long = 0L
  override def child(name: String): DedupFile = new VirtualFile(fs, path / name)
  override def children: Seq[DedupFile] = Seq()
  override def parent: Option[DedupFile] = ???
}

final class ActualFile(fs: FileSystem, val path: String, entry: TreeEntry) extends DedupFile {
  override def toString: String = s"$entry - $path"
  override def name: String = entry.name
  override def mkDir(): Boolean = false
  override def exists: Boolean = true
  override def isDirectory: Boolean = entry.data.isEmpty
  override def isFile: Boolean = entry.data.nonEmpty
  override def isWritable: Boolean = !fs.isReadOnly && !(entry == TreeEntry.root)
  override def size: Long = entry.data flatMap fs.repository.metaBackend.sizeOf getOrElse 0L
  override def lastModified: Long = entry.changed getOrElse 0L
  override def child(name: String): DedupFile = fs.repository.metaBackend.child(entry.key, name) match {
    case None => new VirtualFile(fs, path / name)
    case Some(child) => new ActualFile(fs, path / name, child)
  }
  override def children: Seq[DedupFile] =
    fs.repository.metaBackend.children(entry.key) map (child => new ActualFile(fs, path / child.name, child))
  override def parent: Option[DedupFile] = fs.getFile(entry.parent)
}
