package net.diet_rich.dedupfs

import jnr.ffi.{Platform, Pointer}
import net.diet_rich.bytestore.{ByteStore, MemoryByteStore}
import net.diet_rich.dedup.metaH2.{Database, H2, H2MetaBackend}
import net.diet_rich.dedupfs.FuseConstants._
import net.diet_rich.util.fs._
import net.diet_rich.util.sql.ConnectionFactory
import net.diet_rich.util.{ClassLogging, Head, Nel}
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs}
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}

object SqlFS {
  val separator = "/"
  val rootPath  = "/"

  def split(path: String): Seq[String] = {
    require(path.startsWith(separator), s"Illegal path $path")
    if (path == rootPath) Seq() else path.split(separator).toSeq.drop(1)
  }
}

// FIXME FUSE provides a "file handle" in the "fuse_file_info" structure. The file handle
// FIXME is stored in the "fh" element of that structure, which is an unsigned 64-bit
// FIXME integer (uint64_t) uninterpreted by FUSE. If you choose to use it, you should set
// FIXME that field in your open, create, and opendir functions; other functions can then use it.

/** See for example https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html */
class SqlFS extends FuseStubFS with ClassLogging {
  import SqlFS.split

  private implicit val connectionFactory: ConnectionFactory =
    ConnectionFactory(H2.jdbcMemoryUrl, H2.defaultUser, H2.defaultPassword, H2.memoryOnShutdown)
  Database.create("MD5", Map())
  protected val meta: H2MetaBackend = new H2MetaBackend

  private def sync[T](t: T): T = synchronized(t)
  protected def sync[T](message: => String, asTrace: Boolean = false)(f: => T): T = log(message, asTrace)(sync(f))

  private val bytecache: ByteCache = new ByteCache
  private val bytestore: ByteStore = new MemoryByteStore


  override def flush(path: String, fi: FuseFileInfo): Int =
    sync(s"flush($path, fileInfo)")(super.flush(path, fi))

  override def release(path: String, fi: FuseFileInfo): Int =
    sync(s"release($path, fileInfo)")(super.release(path, fi))

  /** Return file attributes. For the given pathname, this should fill in the elements of the "stat" structure.
    * We might want to fill in additional fields, see https://linux.die.net/man/2/stat and
    * https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html */
  // Note: Calling FileStat.toString DOES NOT WORK
  override def getattr(path: String, stat: FileStat): Int = sync(s"getattr($path, fileStat)", asTrace = true) {
    stat.st_uid.set(getContext.uid.get)
    stat.st_gid.set(getContext.gid.get)
    meta.entries(split(path)) match {
      case Nel(Left(_), _) => ENOENT
      case Nel(Right(entry), _) =>
        if (entry.isDir) { stat.st_mode.set(FileStat.S_IFDIR | O777); OK }
        else {
          stat.st_mode.set(FileStat.S_IFREG | O777)
          stat.st_size.set(files.get(entry.id).map(_.length).getOrElse(0).toInt)
          OK
        }
    }
  }

  /** Attempts to create a directory named [path].
    * EEXIST  Path already exists (not necessarily as a directory).
    * ENOENT  A directory component in pathname does not exist.
    * ENOTDIR A component used as a directory in pathname is not, in fact, a directory. */
  override def mkdir(path: String, mode: Long): Int = sync(s"mkdir($path, $mode)") {
    meta.mkdir(split(path)) match {
        case MkdirOk(_) => OK
        case MkdirParentNotFound => ENOENT
        case MkdirParentNotADir => ENOTDIR
        case MkdirExists => EEXIST
    }
  }

  /** Renames a file, moving it between directories if required. If newpath already exists it will be atomically
    * replaced. oldpath can specify a directory. In this case, newpath must either not exist, or it must specify
    * an empty directory. See https://linux.die.net/man/2/rename */
  override def rename(oldpath: String, newpath: String): Int = sync(s"rename($oldpath, $newpath)") {
    renameImpl(oldpath, newpath) match {
      // EINVAL An attempt was made to make a directory a subdirectory of itself.
      // EISDIR newpath is an existing directory, but oldpath is not a directory.
      // ENOENT The link named by oldpath does not exist; or, a directory component in newpath does not exist; or, oldpath or newpath is an empty string.
      // ENOTDIR A component used as a directory in oldpath or newpath is not, in fact, a directory. Or, oldpath is a directory, and newpath exists but is not a directory.
      // ENOTEMPTY or EEXIST newpath is a nonempty directory, that is, contains entries other than "." and "..".
      case RenameOk => OK
      case RenameNotFound => ENOENT
      case RenameTargetExists => EEXIST
      case RenameParentDoesNotExist => ENOENT
      case RenameParentNotADirectory => ENOTDIR
      case RenameBadPath => EIO
    }
  }

  /** See https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html#readdir-details */
  override def readdir(path: String, buf: Pointer, filler: FuseFillDir, offset: Long, fi: FuseFileInfo): Int =
    sync(s"readdir($path, buffer, filler, $offset, fileInfo)") {
      if (offset.toInt < 0 || offset.toInt != offset) -ErrorCodes.EOVERFLOW
      else {
        meta.entries(split(path)) match {
          case Nel(Left(_), _) => ENOENT
          case Nel(Right(entry), _) =>
            if (!entry.isDir) ENOTDIR else {
              val names = Seq(".", "..") ++ meta.children(entry.id).map(_.name)
              names.zipWithIndex
                .drop(offset.toInt)
                .exists { case (name, k) => filler.apply(buf, name, null, k + 1) != 0 }
              OK
            }
        }
      }
    }

  // FIXME temporary
  private val files: collection.mutable.Map[Long, Array[Byte]] = collection.mutable.Map()

  /** Create and open a file. If the file does not exist, first create it with the specified mode, and then open it.
    * If this method is not implemented or under Linux kernel versions earlier than 2.6.15, the mknod() and open()
    * methods will be called instead.
    * The file handle in the "fh" element of FuseFileInfo is an unsigned 64-bit integer uninterpreted by FUSE.
    * If you choose to use it, you should set that field in your open, create, and opendir functions; other
    * functions can then use it. */
  override def create(path: String, mode: Long, fi: FuseFileInfo): Int = sync(s"create($path, buf, $mode, fileInfo)") {
    meta.entries(split(path)) match {
      case Nel(Left(fileName), Right(parent) :: _) =>
        if (parent.isDir) {
          val (_, dataId) = meta.mkfile(parent.id, fileName)
          bytecache.write(dataId, new Array[Byte](0), 0) // FIXME create method in byte cache???
          OK
        } else EIO
      case Nel(Left(_), _) => ENOENT
      case Nel(Right(entry), _) => if (entry.isDir) EISDIR else OK
    }
  }

  override def write(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    sync(s"write($path, buf, $size, $offset, fileInfo)") {
      // FIXME handle size > MAXINT
      meta.entries(split(path)) match {
        case Nel(Left(_), _) => ENOENT
        case Nel(Right(entry), _) =>
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

  override def read(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    sync(s"read($path, buf, $size, $offset, fileInfo)") {
      meta.entries(split(path)) match {
        case Nel(Left(_), _) => ENOENT
        case Nel(Right(entry), _) =>
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

  override def truncate(path: String, size: Long): Int = sync(s"truncate($path, $size)") {
    meta.entries(split(path)) match {
      case Nel(Left(_), _) => ENOENT
      case Nel(Right(entry), _) =>
        if (entry.isDir) EISDIR
        else {
          val array = files.get(entry.id).map(java.util.Arrays.copyOf(_, size.toInt))
          array.foreach(files += entry.id -> _)
          OK
        }
    }
  }

  // FIXME check https://linux.die.net/man/2/statfs and https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html
  override def statfs(path: String, stbuf: Statvfs): Int = sync(s"statfs($path, buffer)") {
    if (Platform.getNativePlatform.getOS == jnr.ffi.Platform.OS.WINDOWS) {
      // statfs needs to be implemented on Windows in order to allow for copying
      // data from other devices because winfsp calculates the volume size based
      // on the statvfs call.
      // see https://github.com/billziss-gh/winfsp/blob/14e6b402fe3360fdebcc78868de8df27622b565f/src/dll/fuse/fuse_intf.c#L654
      if ("/" == path) {
        // TODO Eventually, we want something meaningful here.
        stbuf.f_blocks.set(1024 * 1024) // total data blocks in file system
        stbuf.f_frsize.set(1024) // fs block size
        stbuf.f_bfree.set(1024 * 1024) // free blocks in fs
      }
    }
    OK
  }

  /** Delete a directory. */ // TODO make sure implementation matches specification with regard to "." and ".."
  override def rmdir(path: String): Int = sync(s"rmdir($path)") {
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
  override def unlink(path: String): Int = sync(s"unlink($path)") {
    delete(path, expectDir = false) match {
      case DeleteOk => OK
      case DeleteHasChildren => EIO // should never happen
      case DeleteNotFound => ENOENT
      case DeleteFileType => EISDIR
      case DeleteBadPath => EIO
    }
  }

  def delete(path: String, expectDir: Boolean): DeleteResult = sync {
    meta.entries(split(path)) match {
      case Nel(Left(_), _) => DeleteNotFound
      case Nel(Right(entry), _) =>
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

  def renameImpl(oldpath: String, newpath: String): RenameResult = sync {
    meta.entries(split(oldpath)) match {
      case Head(Left(_)) => RenameNotFound
      case Head(Right(entry)) =>
        meta.entries(split(newpath)) match {
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
