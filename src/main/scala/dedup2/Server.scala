package dedup2

import dedup2.Level1.{DirEntry1, FileEntry1, TreeEntry1}
import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.Platform.getNativePlatform
import jnr.ffi.Pointer
import org.slf4j.LoggerFactory
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs, Timespec}
import ru.serce.jnrfuse.{FuseFillDir, FuseStubFS}

import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicBoolean

class Server() extends FuseStubFS with FuseConstants {
  private val mountPoint = "TODO" // TODO
  private val readonly = false // TODO
  private val copyWhenMoving = new AtomicBoolean(false)

  private val rights = if (readonly) 365 else 511 // o555 else o777

  private val log = LoggerFactory.getLogger("dedup.Server")
  private val store = new Level1()

  private def guard(msg: String)(f: => Int): Int =
    try f.tap {
      case EINVAL => log.warn(s"EINVAL: $msg")
      case EIO => log.error(s"EIO: $msg")
      case EOVERFLOW => log.warn(s"EOVERFLOW: $msg")
      case result => log.trace(s"$msg -> $result")
    } catch { case e: Throwable => log.error(s"$msg -> ERROR", e); EIO }

  override def umount(): Unit =
    guard(s"Unmount repository from $mountPoint") {
      super.umount()
      store.close()
      log.info(s"Dedup file system is stopped.")
      OK
    }

  /* Note: Calling FileStat.toString DOES NOT WORK, there's a PR: https://github.com/jnr/jnr-ffi/pull/176 */
  override def getattr(path: String, stat: FileStat): Int =
    guard(s"getattr $path") {
      def setCommon(time: Long, nlink: Int): Unit = {
        stat.st_nlink.set(nlink)
        stat.st_mtim.tv_sec.set(time / 1000)
        stat.st_mtim.tv_nsec.set((time % 1000) * 1000000)
        stat.st_uid.set(getContext.uid.get)
        stat.st_gid.set(getContext.gid.get)
      }
      store.entry(path) match {
        case None => ENOENT
        case Some(dir: DirEntry1) =>
          stat.st_mode.set(FileStat.S_IFDIR | rights)
          setCommon(dir.time, 2)
          OK
        case Some(file: FileEntry1) =>
          stat.st_mode.set(FileStat.S_IFREG | rights)
          setCommon(file.time, 1)
          stat.st_size.set(store.size(file))
          OK
      }
    }

  override def utimens(path: String, timespec: Array[Timespec]): Int = if (readonly) EROFS else
    guard(s"utimens $path") { // see man UTIMENSAT(2)
      if (timespec.length < 2) EIO
      else {
        val sec = timespec(1).tv_sec.get
        val nan = timespec(1).tv_nsec.longValue
        if (sec < 0 || nan < 0 || nan > 1000000000) EINVAL
        else store.entry(path) match {
          case None => ENOENT
          case Some(entry) => store.setTime(entry, sec*1000 + nan/1000000); OK
        }
      }
    }

  /* Note: No benefit expected in implementing opendir/releasedir and handing over the file handle to readdir. */
  override def readdir(path: String, buf: Pointer, fill: FuseFillDir, offset: Long, fi: FuseFileInfo): Int =
    guard(s"readdir $path $offset") {
      store.entry(path) match {
        case Some(dir: DirEntry1) =>
          if (offset < 0 || offset.toInt != offset) EOVERFLOW
          else {
            def names = Seq(".", "..") ++ store.children(dir.id).map(_.name)
            // exists: side effect until a condition is met
            names.zipWithIndex.drop(offset.toInt).exists { case (name, k) => fill.apply(buf, name, null, k + 1) != 0 }
            OK
          }
        case Some(_) => ENOTDIR
        case None    => ENOENT
      }
    }

  override def rmdir(path: String): Int = if (readonly) EROFS else guard("rmdir $path") {
    store.entry(path) match {
      case Some(dir: DirEntry1) => if (store.children(dir.id).nonEmpty) ENOTEMPTY else { store.delete(dir); OK }
      case Some(_)              => ENOTDIR
      case None                 => ENOENT
    }
  }

  // Renames a file. Other than the general contract of rename, newpath must not exist.
  override def rename(oldpath: String, newpath: String): Int = if (readonly) EROFS else
    guard(s"rename $oldpath .. $newpath") {
      val (oldParts, newParts) = (store.split(oldpath), store.split(newpath))
      if (oldParts.length == 0 || newParts.length == 0) ENOENT
      else store.entry(oldParts) match {
        case None => ENOENT
        case Some(origin) => store.entry(newParts.take(newParts.length - 1)) match {
          case None => ENOENT
          case Some(_: FileEntry1) => ENOTDIR
          case Some(dir: DirEntry1) =>
            val newName = newParts.last
            store.child(dir.id, newName) match {
              case Some(_) => EEXIST
              case None =>
                def copy(origin: TreeEntry1, newName: String, newParent: Long): Unit =
                  origin match {
                    case file: FileEntry1 => store.copyFile(file, newParent, newName)
                    case dir: DirEntry1 =>
                      val dirId = store.mkDir(newParent, newName)
                      store.children(dir.id).foreach(child => copy(child, child.name, dirId))
                  }
                if (origin.parent != dir.id && copyWhenMoving.get()) copy(origin, newName, dir.id)
                else store.moveRename(origin, dir.id, newName)
                OK
            }
        }
      }
    }

  override def mkdir(path: String, mode: Long): Int = if (readonly) EROFS else guard(s"mkdir $path") {
    val parts = split(path)
    if (parts.length == 0) ENOENT
    else entry(parts.take(parts.length - 1)) match {
      case None => ENOENT
      case Some(_: FileEntry1) => ENOTDIR
      case Some(dir: DirEntry) =>
        val name = parts.last
        db.child(dir.id, name) match {
          case Some(_) => EEXIST
          case None => db.mkDir(dir.id, name); OK
        }
    }
  }

  override def statfs(path: String, stbuf: Statvfs): Int = {
    if (getNativePlatform.getOS == WINDOWS) {
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
  }.tap(r => log.trace(s"statfs $path -> $r"))

  // #########
  // # Files #
  // #########

  private val fileHandles = new FileHandles(tempDir)
  private var startOfFreeData = db.startOfFreeData
  log.info(s"Data stored: ${readableBytes(startOfFreeData)}")
  log.info(s"Dedup file system is started.")

  // 2020.09.18 perfect
  override def create(path: String, mode: Long, fi: FuseFileInfo): Int =
    guard(s"create $path") {
      val parts = split(path)
      if (readonly) EROFS
      else if (parts.length == 0) ENOENT // can't create root
      else entry(parts.dropRight(1)) match { // fetch parent entry
        case None => ENOENT // parent not known
        case Some(_: FileEntry1) => ENOTDIR // parent is a file
        case Some(dir: DirEntry) =>
          val name = parts.last
          db.child(dir.id, name) match {
            case Some(_) => EEXIST // file (or directory with the same name) already exists
            case None =>
              val fileHandle = db.mkFile(dir.id, name, now)
              fi.fh.set(fileHandle); fileHandles.create(fileHandle, Parts(Seq())); OK
          }
      }
    }

  // 2020.09.18 perfect
  override def open(path: String, fi: FuseFileInfo): Int =
    guard(s"open $path") {
      entry(path) match {
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry1) =>
          fi.fh.set(file.id); fileHandles.createOrIncCount(file.id, db.parts(file.dataId)); OK
      }
    }

  // 2020.09.18 good
  override def release(path: String, fi: FuseFileInfo): Int =
    guard(s"release $path") {
      val fileHandle = fi.fh.get()
      db.dataEntry(fileHandle) match {
        case None =>
          log.warn(s"release - no dataid for tree entry $fileHandle (path is $path)")
          ENOENT
        case Some(dataIdOfHandle) =>
          fileHandles.decCount(fileHandle, { entry =>
            // START asynchronously executed release block
            // 1. zero size handling - can be the size was > 0 before...
            if (entry.size == 0) db.setDataId(fileHandle, -1)
            else {
              // 2. calculate hash
              val md = MessageDigest.getInstance(hashAlgorithm)
              entry.read(0, entry.size, lts.read).foreach(md.update)
              val hash = md.digest()
              // 3. check if already known
              db.dataEntry(hash, entry.size) match {
                // 4. already known, simply link
                case Some(dataId) =>
                  log.trace(s"release: $path - content known, linking to dataId $dataId")
                  if (dataIdOfHandle != dataId) db.setDataId(fileHandle, dataId)
                // 5. not yet known, store
                case None =>
                  // 5a. reserve storage space
                  val start = startOfFreeData.tap(_ => startOfFreeData += entry.size)
                  // 5b. write to storage
                  entry.read(0, entry.size, lts.read).foldLeft(0L) { case (position, data) =>
                    lts.write(start + position, data)
                    position + data.length
                  }
                  // 5c. create data entry
                  val dataId = db.newDataIdFor(fileHandle)
                  db.insertDataEntry(dataId, 1, entry.size, start, start + entry.size, hash)
                  log.trace(s"release: $path - new content, dataId $dataId")
              }
            }
            // END asynchronously executed release block
          })
          OK
      }
    }

  // 2020.09.18 perfect
  override def write(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    guard(s"write $path .. offset = $offset, size = $size") {
      val intSize = size.toInt.abs
      if (readonly) EROFS
      else if (offset < 0 || size != intSize) EOVERFLOW
      else {
        val fileHandle = fi.fh.get()
        db.dataEntry(fileHandle) match {
          case None =>
            log.warn(s"write - no dataid for tree entry $fileHandle (path is $path)")
            ENOENT
          case Some(dataIdOfHandle) =>
            fileHandles.cacheEntry(fileHandle) match {
              case None => EIO // write is hopefully only called after create or open
              case Some(cacheEntry) =>
                def dataSource(off: Long, size: Int): Array[Byte] =
                  new Array[Byte](size).tap(data => buf.get(off, data, 0, size))
                cacheEntry.write(offset, size, dataSource)
                intSize
            }
        }
      }
    }

  // 2020.09.18 perfect
  override def read(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    guard(s"read $path .. offset = $offset, size = $size") {
      val intSize = size.toInt.abs
      if (offset < 0 || size != intSize) EOVERFLOW
      else {
        val fileHandle = fi.fh.get()
        db.dataEntry(fileHandle) match {
          case None =>
            log.warn(s"read - no dataid for tree entry $fileHandle (path is $path)")
            ENOENT
          case Some(dataIdOfHandle) =>
            fileHandles.cacheEntry(fileHandle) match {
              case None => EIO // read is hopefully only called after create or open
              case Some(cacheEntry) =>
                cacheEntry.read(offset, size, lts.read).foldLeft(0) { case (position, data) =>
                  buf.put(position, data, 0, data.length)
                  position + data.length
                }
            }
        }
      }
    }

  // 2020.09.18 perfect
  override def truncate(path: String, size: Long): Int =
    guard(s"truncate $path .. $size") {
      if (readonly) EROFS
      else entry(path) match {
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry1) =>
          fileHandles.cacheEntry(file.id) match {
          case None => EIO // truncate is hopefully only called after create or open
          case Some(cacheEntry) => cacheEntry.truncate(size); OK
        }
      }
    }

  // 2020.09.18 perfect
  override def unlink(path: String): Int =
    guard(s"unlink $path") {
      if (readonly) EROFS
      else entry(path) match {
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry1) =>
          if (!db.delete(file.id)) EIO
          else {
            log.trace(s"unlink() - drop handle for ${file.id} $path")
            fileHandles.delete(file.id)
            OK
          }
      }
    }
}
