package dedup2

import java.io.File
import java.lang.System.{currentTimeMillis => now}

import dedup2.Database._
import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.{Platform, Pointer}
import org.slf4j.LoggerFactory
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs}
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}

object Server extends App {
  val mountPoint = "J:\\"
  val repo = new File("")
  val fs = new Server(repo)
  try fs.mount(java.nio.file.Paths.get(mountPoint), true, false)
  finally { fs.umount(); println(s"Repository unmounted from $mountPoint.") }
}

class Server(maybeRelativeRepo: File) extends FuseStubFS {
  private val repo = maybeRelativeRepo.getAbsoluteFile // absolute needed e.g. for getFreeSpace()
  private val db = new Database(H2.mem().tap(Database.initialize))
  private val dataDir = new File(repo, "data").tap{d => d.mkdirs(); require(d.isDirectory)}
  private val store = new DataStore(dataDir.getAbsolutePath, readOnly = false)
  private val log = LoggerFactory.getLogger(getClass)

  private def O777 = 511
  private def sync[T](f: => T): T = synchronized(try f catch { case e: Throwable => log.error("ERROR", e); throw e })
  private def split(path: String): Array[String] = path.split("/").filter(_.nonEmpty)
  private def entry(path: String): Option[TreeEntry] = entry(split(path))
  private def entry(path: Array[String]): Option[TreeEntry] =
    path.foldLeft[Option[TreeEntry]](Some(Database.root)) {
      case (Some(dir: DirEntry), name) => db.child(dir.id, name)
      case _ => None
    }

  override def umount(): Unit = sync { log.info(s"umount") }

  /* Note: Calling FileStat.toString DOES NOT WORK, there's a PR: https://github.com/jnr/jnr-ffi/pull/176 */
  override def getattr(path: String, stat: FileStat): Int = sync {
    def setCommon(time: Long, nlink: Int): Unit = {
      stat.st_nlink.set(nlink)
      stat.st_mtim.tv_sec.set(time / 1000)
      stat.st_mtim.tv_nsec.set((time % 1000) * 1000000)
      stat.st_uid.set(getContext.uid.get)
      stat.st_gid.set(getContext.gid.get)
    }
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(dir: DirEntry) =>
        stat.st_mode.set(FileStat.S_IFDIR | O777)
        setCommon(dir.time, 2)
        0
      case Some(file: FileEntry) =>
        log.info(s"file $path -> $file")
        log.info(s"count for $path / ${file.id} -> ${fileDescriptors.get(file.id)}")
        val (start, stop) = db.startStop(file.dataId)
        val size = store.size(file.id, file.dataId, start, stop)
        stat.st_mode.set(FileStat.S_IFREG | O777)
        setCommon(file.time, 1)
        stat.st_size.set(size)
        0
    }
  }.tap(r => log.debug(s"getattr $path -> $r"))

  /* Note: No benefit expected in implementing opendir/releasedir and handing over the file handle to readdir. */
  override def readdir(path: String, buf: Pointer, fill: FuseFillDir, offset: Long, fi: FuseFileInfo): Int = sync {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: FileEntry) => -ErrorCodes.ENOTDIR
      case Some(dir: DirEntry) =>
        if (offset < 0 || offset.toInt != offset) -ErrorCodes.EOVERFLOW
        else {
          def names = Seq(".", "..") ++ db.children(dir.id).map(_.name)
          // exists: side effect until a condition is met
          names.zipWithIndex.drop(offset.toInt).exists { case (name, k) => fill.apply(buf, name, null, k + 1) != 0 }
          0
        }
    }
  }.tap(r => log.debug(s"readdir $path $offset -> $r"))

  override def rmdir(path: String): Int = sync {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: FileEntry) => -ErrorCodes.ENOTDIR
      case Some(dir: DirEntry) =>
        if (db.children(dir.id).nonEmpty) -ErrorCodes.ENOTEMPTY
        else if (db.delete(dir.id)) 0 else -ErrorCodes.EIO
    }
  }.tap(r => log.info(s"rmdir $path -> $r"))

  // Renames a file. Other than the general contract of rename, newpath must not exist.
  override def rename(oldpath: String, newpath: String): Int = sync {
    val (oldParts, newParts) = (split(oldpath), split(newpath))
    if (oldParts.length == 0 || newParts.length == 0) -ErrorCodes.ENOENT
    else entry(oldParts) match {
      case None => -ErrorCodes.ENOENT
      case Some(source) =>
        entry(newParts.take(newParts.length - 1)) match {
          case None => -ErrorCodes.ENOENT
          case Some(_: FileEntry) => -ErrorCodes.ENOTDIR
          case Some(dir: DirEntry) =>
            val newName = newParts.last
            db.child(dir.id, newName) match {
              case Some(_) => -ErrorCodes.EEXIST
              case None => db.moveRename(source.id, dir.id, newName); 0
            }
        }
    }
  }.tap(r => log.info(s"rename $oldpath -> $newpath -> $r"))

  override def mkdir(path: String, mode: Long): Int = sync {
    val parts = split(path)
    if (parts.length == 0) -ErrorCodes.ENOENT
    else entry(parts.take(parts.length - 1)) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: FileEntry) => -ErrorCodes.ENOTDIR
      case Some(dir: DirEntry) =>
        val name = parts.last
        db.child(dir.id, name) match {
          case Some(_) => -ErrorCodes.EEXIST
          case None => db.mkDir(dir.id, name); 0
        }
    }
  }.tap(r => log.info(s"mkdir $path -> $r"))

  override def statfs(path: String, stbuf: Statvfs): Int = {
    if (Platform.getNativePlatform.getOS == WINDOWS) {
      // statfs needs to be implemented on Windows in order to allow for copying data from
      // other devices because winfsp calculates the volume size based on the statvfs call.
      // see https://github.com/billziss-gh/winfsp/blob/14e6b402fe3360fdebcc78868de8df27622b565f/src/dll/fuse/fuse_intf.c#L654
      if ("/" == path) {
        stbuf.f_blocks.set(dataDir.getTotalSpace / 32768) // total data blocks in file system
        stbuf.f_frsize.set(32768) // fs block size
        stbuf.f_bfree.set(dataDir.getFreeSpace / 32768) // free blocks in fs
      }
    }
    super.statfs(path, stbuf)
  }.tap(r => log.debug(s"statfs $path -> $r"))

  // #########
  // # Files #
  // #########

  private var fileDescriptors: Map[Long, Int] = Map()
  private def incCount(id: Long): Unit = fileDescriptors += id -> (fileDescriptors.getOrElse(id, 0) + 1)
  private def decCount(id: Long): Unit = fileDescriptors.get(id).foreach {
    case 1 => fileDescriptors -= id
    case n => fileDescriptors += id -> (n - 1)
  }

  override def create(path: String, mode: Long, fi: FuseFileInfo): Int = sync {
    val parts = split(path)
    if (parts.length == 0) -ErrorCodes.ENOENT
    else entry(parts.take(parts.length - 1)) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: FileEntry) => -ErrorCodes.ENOTDIR
      case Some(dir: DirEntry) =>
        val name = parts.last
        db.child(dir.id, name) match {
          case Some(_) => -ErrorCodes.EEXIST
          case None =>
            val id = db.mkFile(dir.id, name, now)
            incCount(id)
            log.info(s"count for $path / $id -> ${fileDescriptors.get(id)}")
            0
        }
    }
  }.tap(r => log.info(s"create $path -> $r"))

  override def open(path: String, fi: FuseFileInfo): Int = sync {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: DirEntry) => -ErrorCodes.EISDIR
      case Some(file: FileEntry) =>
        incCount(file.id);
        log.info(s"count for $path / ${file.id} -> ${fileDescriptors.get(file.id)}")
        0
    }
  }.tap(r => log.info(s"open $path -> $r"))

  override def release(path: String, fi: FuseFileInfo): Int = sync {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: DirEntry) => -ErrorCodes.EISDIR
      case Some(file: FileEntry) =>
        decCount(file.id);
        log.info(s"count for $path / ${file.id} -> ${fileDescriptors.get(file.id)}")
        0
    }
  }.tap(r => log.info(s"release $path -> $r"))

  override def write(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = sync {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: DirEntry) => -ErrorCodes.EISDIR
      case Some(file: FileEntry) =>
        val intSize = size.toInt.abs
        if (!fileDescriptors.contains(file.id)) -ErrorCodes.EIO
        else if (offset < 0 || size != intSize) -ErrorCodes.EOVERFLOW
        else {
          log.info(s"count for $path / ${file.id} -> ${fileDescriptors.get(file.id)}")
          val data = new Array[Byte](intSize)
          buf.get(0, data, 0, intSize)
          store.write(file.id, file.dataId)(offset, data)
          intSize
        }
    }
  }.tap(r => log.info(s"write $path -> $offset/$size -> $r"))

  override def read(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = sync {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: DirEntry) => -ErrorCodes.EISDIR
      case Some(file: FileEntry) =>
        if (!fileDescriptors.contains(file.id)) -ErrorCodes.EIO
        else {
          val intSize = size.toInt.abs
          if (offset < 0 || intSize != size) -ErrorCodes.EOVERFLOW
          else {
            log.info(s"count for $path / ${file.id} -> ${fileDescriptors.get(file.id)}")
            val (start, stop) = db.startStop(file.dataId)
            val bytes: Array[Byte] = store.read(file.id, file.dataId, start, stop)(offset, intSize)
            buf.put(0, bytes, 0, bytes.length)
            bytes.length
          }
        }
    }
  }.tap(r => log.debug(s"read $path -> $size/$offset -> $r"))

  override def truncate(path: String, size: Long): Int = sync {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: DirEntry) => -ErrorCodes.EISDIR
      case Some(file: FileEntry) =>
        val (start, stop) = db.startStop(file.dataId)
        store.truncate(file.id, file.dataId, start, stop)
        0
    }
  }.tap(r => log.info(s"truncate $path -> $size -> $r"))

  override def unlink(path: String): Int = sync {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: DirEntry) => -ErrorCodes.EISDIR
      case Some(file: FileEntry) =>
        if (fileDescriptors.contains(file.id)) -ErrorCodes.EBUSY
        else if (db.delete(file.id)) { store.delete(file.id, file.dataId); 0 } else -ErrorCodes.EIO
    }
  }.tap(r => log.info(s"unlink $path -> $r"))
}
