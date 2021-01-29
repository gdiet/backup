package dedup2

import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.Platform.getNativePlatform
import jnr.ffi.Pointer
import org.slf4j.LoggerFactory
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs, Timespec}
import ru.serce.jnrfuse.{FuseFillDir, FuseStubFS}

import java.io.File

object Server extends App {
  private val log = LoggerFactory.getLogger("dedup.Dedup")

  val (options, commands) = args.partition(_.contains("=")).pipe { case (options, commands) =>
    options.map(_.split("=", 2).pipe(o => o(0).toLowerCase -> o(1))).toMap ->
      commands.toList.map(_.toLowerCase())
  }

  require(commands.isEmpty || commands == List("write"), s"Unexpected command(s): $commands")
  val readonly = !commands.contains("write")
  val mountPoint = options.getOrElse("mount", if (getNativePlatform.getOS == WINDOWS) "J:\\" else "/tmp/mnt")
  if (getNativePlatform.getOS != WINDOWS) {
    val mountDir = new File(mountPoint)
    require(mountDir.isDirectory, s"Mount point is not a directory: $mountPoint")
    require(mountDir.list.isEmpty, s"Mount point is not empty: $mountPoint")
  }
  log.info (s"Starting dedup file system.")
  log.info (s"Mount point: $mountPoint")
  log.info (s"Readonly:    $readonly")
  val fs = new Server(Settings(readonly))
  val fuseOptions: Array[String] = if (getNativePlatform.getOS == WINDOWS) Array("-o", "volname=DedupFS") else Array("-o", "fsname=DedupFS")
  try fs.mount(java.nio.file.Paths.get(mountPoint), true, false, fuseOptions)
  catch { case e: Throwable => log.error("Mount exception:", e); fs.umount() }

  /*
  WINDOWS

  FUSE options:
      -h   --help            print help
      -V   --version         print version
      -d   -o debug          enable debug output (implies -f)
      -f                     foreground operation
      -s                     disable multi-threaded operation
      -o opt,[opt...]        mount options

  WinFsp-FUSE options:
      -o umask=MASK              set file permissions (octal)
      -o create_umask=MASK       set newly created file permissions (octal)
          -o create_file_umask=MASK      for files only
          -o create_dir_umask=MASK       for directories only
      -o uid=N                   set file owner (-1 for mounting user id)
      -o gid=N                   set file group (-1 for mounting user group)
      -o rellinks                interpret absolute symlinks as volume relative
      -o dothidden               dot files have the Windows hidden file attrib
      -o volname=NAME            set volume label
      -o VolumePrefix=UNC        set UNC prefix (/Server/Share)
          --VolumePrefix=UNC     set UNC prefix (\Server\Share)
      -o FileSystemName=NAME     set file system name
      -o DebugLog=FILE           debug log file (requires -d)

  WinFsp-FUSE advanced options:
      -o FileInfoTimeout=N       metadata timeout (millis, -1 for data caching)
      -o DirInfoTimeout=N        directory info timeout (millis)
      -o EaTimeout=N             extended attribute timeout (millis)
      -o VolumeInfoTimeout=N     volume info timeout (millis)
      -o KeepFileCache           do not discard cache when files are closed
      -o ThreadCount             number of file system dispatcher threads
  */

  /*
LINUX

usage: fusefs-1292035939 mountpoint [options]

general options:
    -o opt,[opt...]        mount options
    -h   --help            print help
    -V   --version         print version

FUSE options:
    -d   -o debug          enable debug output (implies -f)
    -f                     foreground operation
    -s                     disable multi-threaded operation

    -o allow_other         allow access to other users
    -o allow_root          allow access to root
    -o auto_unmount        auto unmount on process termination
    -o nonempty            allow mounts over non-empty file/dir
    -o default_permissions enable permission checking by kernel
    -o fsname=NAME         set filesystem name
    -o subtype=NAME        set filesystem type
    -o large_read          issue large read requests (2.4 only)
    -o max_read=N          set maximum size of read requests

    -o hard_remove         immediate removal (don't hide files)
    -o use_ino             let filesystem set inode numbers
    -o readdir_ino         try to fill in d_ino in readdir
    -o direct_io           use direct I/O
    -o kernel_cache        cache files in kernel
    -o [no]auto_cache      enable caching based on modification times (off)
    -o umask=M             set file permissions (octal)
    -o uid=N               set file owner
    -o gid=N               set file group
    -o entry_timeout=T     cache timeout for names (1.0s)
    -o negative_timeout=T  cache timeout for deleted names (0.0s)
    -o attr_timeout=T      cache timeout for attributes (1.0s)
    -o ac_attr_timeout=T   auto cache timeout for attributes (attr_timeout)
    -o noforget            never forget cached inodes
    -o remember=T          remember cached inodes for T seconds (0s)
    -o nopath              don't supply path if not necessary
    -o intr                allow requests to be interrupted
    -o intr_signal=NUM     signal to send on interrupt (10)
    -o modules=M1[:M2...]  names of modules to push onto filesystem stack

    -o max_write=N         set maximum size of write requests
    -o max_readahead=N     set maximum readahead
    -o max_background=N    set number of maximum background requests
    -o congestion_threshold=N  set kernel's congestion threshold
    -o async_read          perform reads asynchronously (default)
    -o sync_read           perform reads synchronously
    -o atomic_o_trunc      enable atomic open+truncate support
    -o big_writes          enable larger than 4kB writes
    -o no_remote_lock      disable remote file locking
    -o no_remote_flock     disable remote file locking (BSD)
    -o no_remote_posix_lock disable remove file locking (POSIX)
    -o [no_]splice_write   use splice to write to the fuse device
    -o [no_]splice_move    move data while splicing to the fuse device
    -o [no_]splice_read    use splice to read from the fuse device

Module options:

[iconv]
    -o from_code=CHARSET   original encoding of file names (default: UTF-8)
    -o to_code=CHARSET	    new encoding of the file names (default: UTF-8)

[subdir]
    -o subdir=DIR	    prepend this directory to all paths (mandatory)
    -o [no]rellinks	    transform absolute symlinks to relative
   */
}

class Server(settings: Settings) extends FuseStubFS with FuseConstants {
  import settings.{copyWhenMoving, readonly}

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
        case Some(dir: DirEntry) =>
          stat.st_mode.set(FileStat.S_IFDIR | rights)
          setCommon(dir.time, 2)
          OK
        case Some(file: FileEntry) =>
          stat.st_mode.set(FileStat.S_IFREG | rights)
          setCommon(file.time, 1)
          stat.st_size.set(store.size(file.dataId))
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
          case Some(entry) => store.setTime(entry.id, sec*1000 + nan/1000000); OK
        }
      }
    }

  /* Note: No benefit expected in implementing opendir/releasedir and handing over the file handle to readdir. */
  override def readdir(path: String, buf: Pointer, fill: FuseFillDir, offset: Long, fi: FuseFileInfo): Int =
    guard(s"readdir $path $offset") {
      store.entry(path) match {
        case Some(dir: DirEntry) =>
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
      case Some(dir: DirEntry) => if (store.children(dir.id).nonEmpty) ENOTEMPTY else { store.delete(dir); OK }
      case Some(_)             => ENOTDIR
      case None                => ENOENT
    }
  }

  // Renames a file. Other than the general contract of rename, newpath must not exist.
  // If copyWhenMoving is active, the last persisted state of files is copied - without the current modifications.
  override def rename(oldpath: String, newpath: String): Int = if (readonly) EROFS else
    guard(s"rename $oldpath .. $newpath") {
      val (oldParts, newParts) = (store.split(oldpath), store.split(newpath))
      if (oldParts.length == 0 || newParts.length == 0) ENOENT
      else store.entry(oldParts) match {
        case None => ENOENT
        case Some(origin) => store.entry(newParts.take(newParts.length - 1)) match {
          case None => ENOENT
          case Some(_  : FileEntry) => ENOTDIR
          case Some(dir: DirEntry ) =>
            val newName = newParts.last
            store.child(dir.id, newName) match {
              case Some(_) => EEXIST
              case None =>
                def copy(origin: TreeEntry, newName: String, newParentId: Long): Unit =
                  origin match {
                    case file: FileEntry => store.copyFile(file, newParentId, newName)
                    case dir : DirEntry  =>
                      val dirId = store.mkDir(newParentId, newName)
                      store.children(dir.id).foreach(child => copy(child, child.name, dirId))
                  }
                if (origin.parentId != dir.id && copyWhenMoving.get()) copy(origin, newName, dir.id)
                else store.update(origin.id, dir.id, newName)
                OK
            }
        }
      }
    }

  override def mkdir(path: String, mode: Long): Int = if (readonly) EROFS else guard(s"mkdir $path") {
    val parts = store.split(path)
    if (parts.length == 0) ENOENT
    else store.entry(parts.take(parts.length - 1)) match {
      case None => ENOENT
      case Some(_: FileEntry) => ENOTDIR
      case Some(dir: DirEntry) =>
        val name = parts.last
        store.child(dir.id, name) match {
          case Some(_) => EEXIST
          case None => store.mkDir(dir.id, name); OK
        }
    }
  }

  override def statfs(path: String, stbuf: Statvfs): Int = {
    if (getNativePlatform.getOS == WINDOWS) {
      // statfs needs to be implemented on Windows in order to allow for copying data from
      // other devices because winfsp calculates the volume size based on the statvfs call.
      // see https://github.com/billziss-gh/winfsp/blob/14e6b402fe3360fdebcc78868de8df27622b565f/src/dll/fuse/fuse_intf.c#L654
      if ("/" == path) {
// TODO
//        stbuf.f_blocks.set(dataDir.getTotalSpace / 32768) // total data blocks in file system
//        stbuf.f_frsize.set(32768) // fs block size
//        stbuf.f_bfree.set(dataDir.getFreeSpace / 32768) // free blocks in fs
      }
    }
    super.statfs(path, stbuf)
  }.tap(r => log.trace(s"statfs $path -> $r"))

  // #########
  // # Files #
  // #########

  override def create(path: String, mode: Long, fi: FuseFileInfo): Int = if (readonly) EROFS else
    guard(s"create $path") {
      val parts = store.split(path)
      if (parts.length == 0) ENOENT // can't create root
      else store.entry(parts.dropRight(1)) match { // fetch parent entry
        case None => ENOENT // parent not known
        case Some(_: FileEntry)  => ENOTDIR // parent is a file
        case Some(dir: DirEntry) =>
          val name = parts.last
          store.child(dir.id, name) match {
            case Some(_) => EEXIST // entry with the given name already exists
            case None => fi.fh.set(store.createAndOpen(dir.id, name, now)); OK
          }
      }
    }

  override def open(path: String, fi: FuseFileInfo): Int =
    guard(s"open $path") {
      store.entry(path) match {
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) => store.open(file); fi.fh.set(file.id); OK
      }
    }

  // 2020.09.18 good
  override def release(path: String, fi: FuseFileInfo): Int = EIO
//    guard(s"release $path") {
//      val fileHandle = fi.fh.get()
//      db.dataEntry(fileHandle) match {
//        case None =>
//          log.warn(s"release - no dataid for tree entry $fileHandle (path is $path)")
//          ENOENT
//        case Some(dataIdOfHandle) =>
//          fileHandles.decCount(fileHandle, { entry =>
//            // START asynchronously executed release block
//            // 1. zero size handling - can be the size was > 0 before...
//            if (entry.size == 0) db.setDataId(fileHandle, -1)
//            else {
//              // 2. calculate hash
//              val md = MessageDigest.getInstance(hashAlgorithm)
//              entry.read(0, entry.size, lts.read).foreach(md.update)
//              val hash = md.digest()
//              // 3. check if already known
//              db.dataEntry(hash, entry.size) match {
//                // 4. already known, simply link
//                case Some(dataId) =>
//                  log.trace(s"release: $path - content known, linking to dataId $dataId")
//                  if (dataIdOfHandle != dataId) db.setDataId(fileHandle, dataId)
//                // 5. not yet known, store
//                case None =>
//                  // 5a. reserve storage space
//                  val start = startOfFreeData.tap(_ => startOfFreeData += entry.size)
//                  // 5b. write to storage
//                  entry.read(0, entry.size, lts.read).foldLeft(0L) { case (position, data) =>
//                    lts.write(start + position, data)
//                    position + data.length
//                  }
//                  // 5c. create data entry
//                  val dataId = db.newDataIdFor(fileHandle)
//                  db.insertDataEntry(dataId, 1, entry.size, start, start + entry.size, hash)
//                  log.trace(s"release: $path - new content, dataId $dataId")
//              }
//            }
//            // END asynchronously executed release block
//          })
//          OK
//      }
//    }

  override def write(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = if (readonly) EROFS else
    guard(s"write $path .. offset = $offset, size = $size") {
      val intSize = size.toInt.abs
      if (offset < 0 || size != intSize) EOVERFLOW else {
        val fileHandle = fi.fh.get()
        def dataSource(off: Long, size: Int): Array[Byte] =
          new Array[Byte](size).tap(data => buf.get(off, data, 0, size))
        if (store.write(fileHandle, offset, size, dataSource)) intSize else EIO
      }
    }

  // 2020.09.18 perfect
  override def read(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = EIO
//    guard(s"read $path .. offset = $offset, size = $size") {
//      val intSize = size.toInt.abs
//      if (offset < 0 || size != intSize) EOVERFLOW
//      else {
//        val fileHandle = fi.fh.get()
//        db.dataEntry(fileHandle) match {
//          case None =>
//            log.warn(s"read - no dataid for tree entry $fileHandle (path is $path)")
//            ENOENT
//          case Some(dataIdOfHandle) =>
//            fileHandles.cacheEntry(fileHandle) match {
//              case None => EIO // read is hopefully only called after create or open
//              case Some(cacheEntry) =>
//                cacheEntry.read(offset, size, lts.read).foldLeft(0) { case (position, data) =>
//                  buf.put(position, data, 0, data.length)
//                  position + data.length
//                }
//            }
//        }
//      }
//    }

  // 2020.09.18 perfect
  override def truncate(path: String, size: Long): Int = EIO
//    guard(s"truncate $path .. $size") {
//      if (readonly) EROFS
//      else entry(path) match {
//        case None => ENOENT
//        case Some(_: DirEntry) => EISDIR
//        case Some(file: FileEntry1) =>
//          fileHandles.cacheEntry(file.id) match {
//          case None => EIO // truncate is hopefully only called after create or open
//          case Some(cacheEntry) => cacheEntry.truncate(size); OK
//        }
//      }
//    }

  // 2020.09.18 perfect
  override def unlink(path: String): Int = EIO
//    guard(s"unlink $path") {
//      if (readonly) EROFS
//      else entry(path) match {
//        case None => ENOENT
//        case Some(_: DirEntry) => EISDIR
//        case Some(file: FileEntry1) =>
//          if (!db.delete(file.id)) EIO
//          else {
//            log.trace(s"unlink() - drop handle for ${file.id} $path")
//            fileHandles.delete(file.id)
//            OK
//          }
//      }
//    }
}
