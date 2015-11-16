package net.diet_rich.dedupfs

import java.io.{FileNotFoundException, InputStream, OutputStream, IOException}

import net.diet_rich.common._
import net.diet_rich.dedupfs.metadata.TreeEntry, TreeEntry.RichPath

/** FileSystem is a convenience wrapper for the repository providing methods
  * that resemble more the standard file system API methods. */
class FileSystem(val repository: Repository.Any) extends AutoCloseable { import repository._
  private val maybeRepositoryReadWrite = repository match { case r: Repository => Some(r); case _ => None }
  val isReadOnly = maybeRepositoryReadWrite.isEmpty
  val root = new ActualFile(this, TreeEntry.rootPath, TreeEntry.root)

  private def write[T](f: Repository => T): T =
    maybeRepositoryReadWrite match {
      case None => throw new IOException("File system is read-only")
      case Some(repositoryReadWrite) => f(repositoryReadWrite)
    }

  def mkDir(path: String): Boolean = write { write =>
    val pathElements = path.pathElements
    val (parentPath, Array(name)) = pathElements splitAt (pathElements.length - 1)
    metaBackend entry parentPath match {
      case Some(parent) => write.metaBackend create (parent.key, name, someNow, None); true
      case None => false
    }
  }

  def delete(entry: TreeEntry): Unit = write { _.metaBackend.delete(entry) }

  def getFile(path: String): DedupFile =
    metaBackend entry path match {
      case None => new VirtualFile(this, path)
      case Some(entry) => new ActualFile(this, path, entry)
    }

  def getFile(key: Long): Option[DedupFile] =
    metaBackend.entry(key) flatMap { parent =>
      metaBackend.path(parent.key) map (new ActualFile(this, _, parent))
    }

  /** Note: Storing a Source is the preferred (more optimized) way for storing data. */
  def createWithOutputStream(parentKey: Long, name: String, changed: Option[Long] = someNow): OutputStream = write { write =>
    new StoreOutputStream(write.storeLogic, { dataid =>
      write.metaBackend.create(parentKey, name, changed, Some(dataid))
    })
  }

  /** Note: Storing a Source is the preferred (more optimized) way for storing data. */
  def replaceWithOutputStream(entry: TreeEntry, changed: Option[Long] = someNow): OutputStream = write { write =>
    new StoreOutputStream(write.storeLogic, { dataid =>
      write.metaBackend.change(entry.copy(data = Some(dataid), changed = changed))
    })
  }

  def getInputStream(dataid: Long): InputStream = repository.read(dataid).asInputStream

  override def close(): Unit = repository.close()
}

trait DedupFile {
  def name: String
  def path: String
  def mkDir(): Boolean
  def parent: Option[DedupFile]
  def child(name: String): DedupFile
  def children: Iterable[DedupFile]
  def isDirectory: Boolean
  def isFile: Boolean
  def exists: Boolean
  def isWritable: Boolean
  def size: Long
  def lastModified: Long
  def delete(): Boolean
  def ouputStream(): OutputStream
  def inputStream(): InputStream
}

final class VirtualFile(fs: FileSystem, val path: String) extends DedupFile {
  override def toString: String = s"virtual:$path"
  override def name: String = path.pathElements.last
  override def mkDir(): Boolean = fs mkDir path
  override def exists: Boolean = false
  override def isDirectory: Boolean = false
  override def isFile: Boolean = false
  override def isWritable: Boolean = ! fs.isReadOnly
  override def size: Long = 0L
  override def lastModified: Long = 0L
  override def child(name: String): DedupFile = new VirtualFile(fs, path / name)
  override def children: Iterable[DedupFile] = Seq()
  override def parent: Option[DedupFile] = Some(fs getFile path.parent)
  override def delete(): Boolean = false
  override def ouputStream(): OutputStream = parent match {
    case Some(f: ActualFile) if f.isDirectory => fs.createWithOutputStream(f.entry.key, name)
    case _ => throw new IOException("Can't create output stream - parent is not a directory.")
  }
  override def inputStream(): InputStream = throw new FileNotFoundException(s"File is virtual, can't create input stream: $path")
}

final class ActualFile(fs: FileSystem, val path: String, private[dedupfs] val entry: TreeEntry) extends DedupFile {
  import fs.repository.{metaBackend => meta}
  override def toString: String = s"$entry - $path"
  override def name: String = entry.name
  override def mkDir(): Boolean = false
  override def exists: Boolean = true
  override def isDirectory: Boolean = entry.data.isEmpty
  override def isFile: Boolean = entry.data.nonEmpty
  override def isWritable: Boolean = !fs.isReadOnly && !(entry == TreeEntry.root)
  override def size: Long = entry.data flatMap meta.sizeOf getOrElse 0L
  override def lastModified: Long = entry.changed getOrElse 0L
  override def child(name: String): DedupFile = meta.child(entry.key, name) match {
    case None => new VirtualFile(fs, path / name)
    case Some(child) => new ActualFile(fs, path / name, child)
  }
  override def children: Iterable[DedupFile] = meta.children(entry.key) map (child => new ActualFile(fs, path / child.name, child))
  override def parent: Option[DedupFile] = fs getFile entry.parent
  override def delete(): Boolean = { fs delete entry; true }
  override def ouputStream(): OutputStream =
    if (isFile) fs.replaceWithOutputStream(entry) else throw new IOException("Can't create output stream for a directory.")
  override def inputStream(): InputStream = entry.data match {
    case None => throw new IOException(s"Can't create input stream for directory: $path")
    case Some(dataid) => fs getInputStream dataid
  }
}
