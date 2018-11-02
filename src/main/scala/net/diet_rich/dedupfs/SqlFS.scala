package net.diet_rich.dedupfs

import jnr.ffi.Pointer
import net.diet_rich.dedup.metaH2.{Database, H2, H2MetaBackend}
import net.diet_rich.util.{ClassLogging, Head, Nel}
import net.diet_rich.util.fs._
import net.diet_rich.util.sql.ConnectionFactory
import ru.serce.jnrfuse.FuseStubFS
import ru.serce.jnrfuse.struct.FuseFileInfo

object SqlFS {
  val separator = "/"
  val rootPath  = "/"

  def pathElements(path: String): Option[List[String]] = {
    if (!path.startsWith(separator)) None else
    if (path == rootPath) Some(List()) else
      Some(path.split(separator).toList.drop(1))
  }

  def splitParentPath(path: String): Option[(String, String)] =
    if (!path.startsWith(separator) || path == rootPath) None else {
      Some(path.lastIndexOf('/') match {
        case 0 => ("/", path.drop(1))
        case i => (path.take(i), path.drop(i+1))
      })
    }
}
// Note: This is not yet a fuse FS, still need to move things from FuseFS here...
class SqlFS extends FuseStubFS with ClassLogging {
  private implicit val connectionFactory: ConnectionFactory =
    ConnectionFactory(H2.jdbcMemoryUrl, H2.defaultUser, H2.defaultPassword, H2.memoryOnShutdown)
  Database.create("MD5", Map())
  protected val meta: H2MetaBackend = new H2MetaBackend

  private def sync[T](t: T): T = synchronized(t)
  protected def sync[T](message: => String)(f: => T): T = log(message)(sync(f))


  import FuseConstants._

  /** Attempts to create a directory named [path].
    * EEXIST  Path already exists (not necessarily as a directory).
    * ENOENT  A directory component in pathname does not exist.
    * ENOTDIR A component used as a directory in pathname is not, in fact, a directory. */
  override def mkdir(path: String, mode: Long): Int = sync {
    SqlFS.pathElements(path).fold[Int](EIO)(meta.mkdir(_) match {
        case MkdirOk(_) => OK
        case MkdirParentNotFound => ENOENT
        case MkdirParentNotADir => ENOTDIR
        case MkdirExists => EEXIST
    })
  }




  // FIXME temporary
  private val files: collection.mutable.Map[Long, Array[Byte]] = collection.mutable.Map()

  /** Create and open a file. If the file does not exist, first create it with the specified mode, and then open it.
    * If this method is not implemented or under Linux kernel versions earlier than 2.6.15, the mknod() and open()
    * methods will be called instead.
    * The file handle in the "fh" element of FuseFileInfo is an unsigned 64-bit integer uninterpreted by FUSE.
    * If you choose to use it, you should set that field in your open, create, and opendir functions; other
    * functions can then use it. */
  override def create(path: String, mode: Long, fi: FuseFileInfo): Int = sync {
    SqlFS.pathElements(path).map(meta.entries) match {
      case None => EIO
      case Some(Nel(Left(fileName), Right(parent) :: _)) =>
        if (parent.isDir) {
          meta.mkfile(parent.id, fileName)
          OK
        } else EIO
      case Some(Nel(Left(_), _)) => ENOENT
      case Some(Nel(Right(entry), _)) =>
        if (entry.isDir) EISDIR
        else OK
    }
  }

  override def write(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = sync {
    // FIXME handle size > MAXINT
    SqlFS.pathElements(path).map(meta.entries) match {
      case None => EIO
      case Some(Nel(Left(_), _)) => ENOENT
      case Some(Nel(Right(entry), _)) =>
        if (entry.isDir) EISDIR
        else {
          val array = {
            val a = files.getOrElse(entry.id, new Array[Byte](0))
            if (a.length >= offset + size) a else java.util.Arrays.copyOf(a, (offset + size).toInt)
          }
          buf.get(0, array, offset.toInt, size.toInt)
          log.info(s"written ${entry.id} -> ${new String(array, "UTF-8")}")
          files += entry.id -> array
          size.toInt
        }
    }
  }

  override def read(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = sync {
    SqlFS.pathElements(path).map(meta.entries) match {
      case None => EIO
      case Some(Nel(Left(_), _)) => ENOENT
      case Some(Nel(Right(entry), _)) =>
        if (entry.isDir) EISDIR
        else {
          val array = files.getOrElse(entry.id, Array())
          val bytesToRead = math.min(array.length - offset, size).toInt
          buf.put(0, array, offset.toInt, bytesToRead)
          log.info(s"read ${entry.id} -> ${new String(array, "UTF-8")}")
          bytesToRead
        }
    }
  }

  override def truncate(path: String, size: Long): Int = sync {
    SqlFS.pathElements(path).map(meta.entries) match {
      case None => EIO
      case Some(Nel(Left(_), _)) => ENOENT
      case Some(Nel(Right(entry), _)) =>
        if (entry.isDir) EISDIR
        else {
          val array = files.get(entry.id).map(java.util.Arrays.copyOf(_, size.toInt))
          array.foreach(files += entry.id -> _)
          OK
        }
    }
  }


  sealed trait Node {
    protected def entry: meta.TreeEntry
    final def name: String = sync(entry.name)
  }

  class Dir(val entry: meta.TreeEntry) extends Node {
    override def toString: String = sync(s"Dir '$name': $entry")
  }

  class File(val entry: meta.TreeEntry) extends Node {
    def size: Long = files.get(entry.id).map(_.length.toLong).getOrElse(0)
    override def toString: String = sync(s"File '$name': $entry")
  }

  private def nodeFor(entry: meta.TreeEntry): Node =
    if (entry.isDir) new Dir(entry) else new File(entry)

  /** Delete a directory. */ // TODO make sure implementation matches specification with regard to "." and ".."
  override def rmdir(path: String): Int = sync {
      delete(path, expectDir = true) match {
        // EINVAL path has . as last component.
        // ENOENT A directory component in path does not exist.
        // ENOTDIR path, or a component used as a directory in pathname, is not, in fact, a directory.
        // ENOTEMPTY path contains entries other than . and .. ; or, path has .. as its final component.
        case DeleteOk => OK
        case DeleteHasChildren => ENOTEMPTY
        case DeleteNotFound => ENOENT
        case DeleteFileType => ENOTDIR
        case DeleteBadPath => EIO
      }
    }

  // FIXME check https://linux.die.net/man/2/unlink and https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html
  override def unlink(path: String): Int = sync {
    delete(path, expectDir = false) match {
      case DeleteOk => OK
      case DeleteHasChildren => EIO // should never happen
      case DeleteNotFound => ENOENT
      case DeleteFileType => EISDIR
      case DeleteBadPath => EIO
    }
  }

  def delete(path: String, expectDir: Boolean): DeleteResult = sync {
    SqlFS.pathElements(path).map(meta.entries) match {
      case None => DeleteBadPath
      case Some(Nel(Left(_), _)) => DeleteNotFound
      case Some(Nel(Right(entry), _)) =>
        if (expectDir) {
          if (!entry.isDir) DeleteFileType
          else if (entry.isDir && meta.children(entry.id).nonEmpty) DeleteHasChildren
          else { meta.delete(entry.id); DeleteOk }
        } else {
          if (entry.isDir) DeleteFileType
          else { meta.delete(entry.id); DeleteOk }
        }
    }
  }

  def getNode(path: String): Option[Node] = sync {
    SqlFS.pathElements(path).map(meta.entries).flatMap(_.head.toOption.map(nodeFor))
  }

  def readdir(path: String): ReaddirResult = sync {
    SqlFS.pathElements(path).map(meta.entries) match {
      case None => ReaddirBadPath
      case Some(Nel(Left(_), _)) => ReaddirNotFound
      case Some(Nel(Right(entry), _)) =>
        if (entry.isDir) ReaddirOk(meta.children(entry.id).map(nodeFor))
        else ReaddirNotADirectory
    }
  }

  def renameImpl(oldpath: String, newpath: String): RenameResult = sync {
    (SqlFS.pathElements(oldpath), SqlFS.pathElements(newpath)) match {
      case (None, _) | (_, None) => RenameBadPath
      case (Some(oldNames), Some(newNames)) =>
        meta.entries(oldNames) match {
          case Head(Left(_)) => RenameNotFound
          case Head(Right(entry)) =>
            meta.entries(newNames) match {
              case Nel(Right(target), Right(newParent) :: _) =>
                if (entry.isDir && target.isDir && meta.children(target.id).isEmpty) {
                  meta.delete(target.id)
                  meta.moveRename(entry.id, target.name, newParent.id)
                } else RenameTargetExists
              case Nel(Left(newName), Right(newParent) :: _) =>
                if (newParent.isDir) meta.moveRename(entry.id, newName, newParent.id)
                else RenameParentNotADirectory
              case Head(Left(_)) => RenameParentDoesNotExist
            }
        }
    }
  }

  sealed trait ReaddirResult
  case class ReaddirOk(children: Seq[Node]) extends ReaddirResult
  case object ReaddirNotFound extends ReaddirResult
  case object ReaddirNotADirectory extends ReaddirResult
  case object ReaddirBadPath extends ReaddirResult
}
