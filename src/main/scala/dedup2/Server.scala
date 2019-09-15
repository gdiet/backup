package dedup2

import java.io.File
import java.lang.System.{currentTimeMillis => now}

import dedup2.Database._
import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.{Platform, Pointer}
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs}
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}

import scala.util.chaining._

object Server extends App {
  val mountPoint = "J:\\"
  val repo = new File("").getAbsoluteFile // absolute needed e.g. for getFreeSpace()
  val fs = new Server(repo)
  try fs.mount(java.nio.file.Paths.get(mountPoint), true, false)
  finally { fs.umount(); println(s"Repository unmounted from $mountPoint.") }
}

class Server(repo: File) extends FuseStubFS {
  private val db = new Database(H2.mem().tap(Database.initialize))
  private val dataDir = new File(repo, "data").tap{f => f.mkdirs(); require(f.isDirectory)}

  private def O777 = 511
  private def sync[T](f: => T): T = synchronized(f)
  private def split(path: String): Array[String] =
    path.split("/").filter(_.nonEmpty)
  private def entry(path: String): Option[TreeEntry] =
    entry(split(path))
  private def entry(path: Array[String]): Option[TreeEntry] =
    path.foldLeft[Option[TreeEntry]](Some(Database.root)) {
      case (Some(dir: DirEntry), name) => db.child(dir.id, name)
      case _ => None
    }

  override def umount(): Unit = sync {}

  /* Note: Calling FileStat.toString DOES NOT WORK, there's a PR: https://github.com/jnr/jnr-ffi/pull/176 */
  override def getattr(path: String, stat: FileStat): Int = sync {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(dir: DirEntry) =>
        stat.st_mtim.tv_nsec.set((dir.time % 1000) * 1000)
        stat.st_mtim.tv_sec.set(dir.time / 1000)
        stat.st_mode.set(FileStat.S_IFDIR | O777)
        stat.st_nlink.set(2)
        0
      case Some(file: FileEntry) =>
        stat.st_mode.set(FileStat.S_IFREG | O777)
        stat.st_nlink.set(1)
        stat.st_size.set(memoryStore.size(file.dataId).getOrElse[Long](db.size(file.dataId)))
        stat.st_mtim.tv_nsec.set((file.time % 1000) * 1000)
        stat.st_mtim.tv_sec.set(file.time / 1000)
        0
    }
  }

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
  }

  override def rmdir(path: String): Int = sync {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: FileEntry) => -ErrorCodes.ENOTDIR
      case Some(dir: DirEntry) => db.delete(dir.id); 0
    }
  }.tap(r => println(s"rmdir $path -> $r"))

  // Renames a file. Other than the general contract of rename, newpath must not exist.
  override def rename(oldpath: String, newpath: String): Int = sync {
    val (oldParts, newParts) = (split(oldpath), split(newpath))
    if (fileDescriptors.keys.exists(k => k == oldpath || k.startsWith(oldpath + "/"))) -ErrorCodes.EBUSY()
    else if (oldParts.length == 0 || newParts.length == 0) -ErrorCodes.ENOENT
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
  }.tap(r => println(s"rename $oldpath -> $newpath -> $r"))

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
  }.tap(r => println(s"mkdir $path -> $r"))

  override def statfs(path: String, stbuf: Statvfs): Int = {
    if (Platform.getNativePlatform.getOS == WINDOWS) {
      // statfs needs to be implemented on Windows in order to allow for copying data from
      // other devices because winfsp calculates the volume size based on the statvfs call.
      // see https://github.com/billziss-gh/winfsp/blob/14e6b402fe3360fdebcc78868de8df27622b565f/src/dll/fuse/fuse_intf.c#L654
      if ("/" == path) {
        stbuf.f_blocks.set(dataDir.getTotalSpace / 8192) // total data blocks in file system
        stbuf.f_frsize.set(8192) // fs block size
        stbuf.f_bfree.set(dataDir.getFreeSpace / 8192) // free blocks in fs
      }
    }
    super.statfs(path, stbuf)
  }

  // #########
  // # files #
  // #########

  private var fileDescriptors: Map[String, Int] = Map()
  private def incCount(path: String): Unit = fileDescriptors += path -> (fileDescriptors.getOrElse(path, 0) + 1)
  private def decCount(path: String): Unit = fileDescriptors.get(path) foreach {
    case 1 => fileDescriptors -= path
    case n => fileDescriptors += path -> (n - 1)
  }
  private val memoryStore = new MemoryStore

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
            val dataId = db.mkDataEntry()
            db.mkFile(dir.id, name, now, dataId)
            incCount(path)
            memoryStore.store(dataId, 0, Array())
            0
        }
    }
  }.tap(r => println(s"create $path -> $r"))

  override def open(path: String, fi: FuseFileInfo): Int = sync {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: DirEntry) => -ErrorCodes.EISDIR
      case Some(_: FileEntry) => incCount(path); 0
    }
  }.tap(r => println(s"open $path ->${fileDescriptors.getOrElse(path, "X")} $r"))

  override def release(path: String, fi: FuseFileInfo): Int = sync {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: DirEntry) => -ErrorCodes.EISDIR
      case Some(_: FileEntry) => decCount(path); 0
    }
  }.tap(r => println(s"release $path ->${fileDescriptors.getOrElse(path, "X")} $r"))

  override def write(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = sync {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: DirEntry) => -ErrorCodes.EISDIR
      case Some(file: FileEntry) =>
        val intSize = size.toInt.abs
        if (!fileDescriptors.contains(path)) -ErrorCodes.EIO
        else if (offset < 0 || size != intSize) -ErrorCodes.EOVERFLOW
        else {
          val data = new Array[Byte](intSize)
          buf.get(0, data, 0, intSize)
          memoryStore.store(file.dataId, offset, data)
          0
        }
    }
  }.tap(r => println(s"write $path ->${fileDescriptors.getOrElse(path, "X")} $offset/$size -> $r"))

  override def read(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = sync {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: DirEntry) => -ErrorCodes.EISDIR
      case Some(file: FileEntry) =>
        if (!fileDescriptors.contains(path)) -ErrorCodes.EIO
        else {
          val intSize = size.toInt.abs
          if (offset < 0 || intSize != size) -ErrorCodes.EOVERFLOW
          else {
            val bytes: Array[Byte] = memoryStore.read(file.dataId, offset, intSize)
            buf.put(0, bytes, 0, bytes.length)
            bytes.length
          }
        }
    }
  }.tap(r => println(s"read $path ->${fileDescriptors.getOrElse(path, "X")} $size/$offset -> $r"))

  override def truncate(path: String, size: Long): Int = sync { // change file size
    -ErrorCodes.EIO
  }.tap(r => println(s"truncate $path -> $size -> $r"))

  override def unlink(path: String): Int = { // delete file
    -ErrorCodes.EIO
  }.tap(r => println(s"unlink $path -> $r"))
}
