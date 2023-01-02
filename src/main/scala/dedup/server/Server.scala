package dedup
package server

import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.{Platform, Pointer}
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs, Timespec}
import ru.serce.jnrfuse.{FuseFillDir, FuseStubFS}

class Server(settings: Settings) extends FuseStubFS with util.ClassLogging:
  private val backend = Backend(settings)

  private val rights =
    if Platform.getNativePlatform.getOS == WINDOWS then
      // Windows needs the executable flag, at least for the root.
      if settings.readonly then 365 else 511 // o555 else o777
    else
      // On Linux, clear the executable flag because it is rather dangerous.
      if settings.readonly then 292 else 438 // o444 else o666

  /** Utility wrapper for fuse server file system methods. */
  protected def fs(msg: => String, logger: (=> String) => Unit = log.trace)(f: => Int): Int =
    watch(msg, logger)(f).tap {
      case EIO       => log.error(s"EIO: $msg")
      case EINVAL    => log.warn (s"EINVAL: $msg")
      case EOVERFLOW => log.warn (s"EOVERFLOW: $msg")
      case _         => /**/
    }

  override def umount(): Unit =
    fs(s"umount") {
      log.info(s"Stopping dedup file system...")
      super.umount()
      log.info(s"File system is stopped, stand by for closing resources...")
      backend.close()
      log.info(s"Resources are closed. Good bye!")
      OK // Needed for the fs() wrapper utility.
    }

  // *************
  // Tree Handling
  // *************

  override def getattr(path: String, stat: FileStat): Int =
    fs(s"getattr $path") {
      def setCommon(time: Time, nlink: Int): Unit =
        stat.st_nlink.set(nlink)
        stat.st_mtim.tv_sec .set (time.asLong / 1000)
        stat.st_mtim.tv_nsec.set((time.asLong % 1000) * 1000000)
        stat.st_uid.set(getContext.uid.get)
        stat.st_gid.set(getContext.gid.get)

      backend.entry(path) match
        case None =>
          ENOENT
        case Some(dir: DirEntry) =>
          stat.st_mode.set(FileStat.S_IFDIR | rights)
          setCommon(dir.time, 2)
          OK
        case Some(file: FileEntry) =>
          stat.st_mode.set(FileStat.S_IFREG | rights)
          setCommon(file.time, 1)
          stat.st_size.set(backend.size(file))
          OK
    }

  override def mkdir(path: String, mode: Long): Int =
    if settings.readonly then EROFS else fs(s"mkdir $path") {
      val parts = backend.pathElements(path)
      if parts.length == 0 then ENOENT else
        backend.entry(parts.dropRight(1)) match
          case None                 => ENOENT
          case Some(_  : FileEntry) => ENOTDIR
          case Some(dir: DirEntry ) => backend.mkDir(dir.id, parts.last).fold(EEXIST)(_ => OK)
    }

  // No benefit expected from implementing opendir/releasedir and handing over a file handle to readdir.
  override def readdir(path: String, buf: Pointer, fill: FuseFillDir, offset: Long, fi: FuseFileInfo): Int =
    fs(s"readdir $path $offset") {
      backend.entry(path) match
        case Some(dir: DirEntry) =>
          if offset < 0 || offset.toInt != offset then EOVERFLOW else
            def names = Seq(".", "..") ++ backend.children(dir.id).map(_.name)
            // '.exists' used for side effect until a condition is met.
            // Providing a FileStat would probably save getattr calls but is not straightforward to implement.
            // The last arg of fill.apply could be set to 0, but then there would be no paging for readdir.
            names.zipWithIndex.drop(offset.toInt).exists { case (name, k) => fill.apply(buf, name, null, k + 1) != 0 }
            OK
        case Some(_: FileEntry) => ENOTDIR
        case None               => ENOENT
    }

  // If copyWhenMoving is active, the last persisted state of files is copied - without any current modifications.
  override def rename(oldpath: String, newpath: String): Int =
    if (settings.readonly) EROFS else fs(s"rename $oldpath .. $newpath") {
      val oldParts = backend.pathElements(oldpath)
      val newParts = backend.pathElements(newpath)
      if oldParts.length == 0 || newParts.length == 0 then ENOENT else
      if oldParts.sameElements(newParts)              then OK     else
        // Prevent race conditions where e.g. the new parent is marked deleted while moving a tree entry there.
        backend.synchronizeTreeModification {
          backend.entry(oldParts) match
            case None         => ENOENT // oldpath does not exist.
            case Some(origin) => backend.entry(newParts.dropRight(1)) match
              case None                       => ENOENT  // Parent of newpath does not exist.
              case Some(_        : FileEntry) => ENOTDIR // Parent of newpath is a file.
              case Some(targetDir: DirEntry ) =>
                val newName = newParts.last
                origin -> backend.child(targetDir.id, newName) match
                  case (_: FileEntry, Some(_: DirEntry)) => EISDIR // oldpath is a file and newpath is a dir.
                  case (_           , previous         ) =>
                    // Other than the contract of rename (see https://linux.die.net/man/2/rename), the
                    // replace operation is not atomic. This is tolerated in order to simplify the code.
                    if !previous.forall(backend.deleteChildless) then ENOTEMPTY
                    else if origin.parentId != targetDir.id && settings.copyWhenMoving.get() then
                      def copy(source: TreeEntry, newName: String, newParentId: Long): Boolean = source match
                        case file: FileEntry =>
                          backend.copyFile(file, newParentId, newName)
                        case dir : DirEntry =>
                          backend.mkDir(newParentId, newName)
                            .exists(dirId => backend.children(dir.id).forall(child => copy(child, child.name, dirId)))
                      if (copy(origin, newName, targetDir.id)) OK else EEXIST
                    else
                      if backend.renameMove(origin.id, targetDir.id, newName) then OK else EEXIST
        }
    }

  override def rmdir(path: String): Int =
    if settings.readonly then EROFS else fs("rmdir $path") {
      backend.entry(path) match
        case Some(dir: DirEntry) => if backend.deleteChildless(dir) then OK else ENOTEMPTY
        case Some(_)             => ENOTDIR
        case None                => ENOENT
    }

  // Implemented for Windows in order to allow for copying data from other devices because winfsp calculates
  // volume size based on statvfs. See ru.serce.jnrfuse.examples.MemoryFS.statfs and
  // https://github.com/billziss-gh/winfsp/blob/14e6b402fe3360fdebcc78868de8df27622b565f/src/dll/fuse/fuse_intf.c#L654
  // On Linux more of the stbuf struct would need to be filled to get sensible disk space values.
  override def statfs(path: String, stbuf: Statvfs): Int =
    fs(s"statfs $path") {
      if Platform.getNativePlatform.getOS == WINDOWS then
        stbuf.f_frsize.set(                                 32768) // fs block size
        stbuf.f_bfree .set(settings.dataDir.getFreeSpace  / 32768) // free blocks in fs
        stbuf.f_blocks.set(settings.dataDir.getTotalSpace / 32768) // total data blocks in file system
      OK
    }

  // see man UTIMENSAT(2)
  override def utimens(path: String, timespec: Array[Timespec]): Int =
    if settings.readonly then EROFS else fs(s"utimens $path") {
      if timespec.length < 2 then EIO else
        val sec = timespec(1).tv_sec .get
        val nan = timespec(1).tv_nsec.longValue
        if sec < 0 || nan < 0 || nan > 1000000000 then EINVAL else
          backend.entry(path) match
            case None        => ENOENT
            case Some(entry) => backend.setTime(entry.id, sec*1000 + nan/1000000); OK
    }

  override def chmod(path: String, mode: Long): Int =
    fs(s"chmod $path $mode") {
      log.trace(s"No-op chmod: $mode -> $path")
      OK
    }

  override def chown(path: String, uid: Long, gid: Long): Int =
    fs(s"chown $path $uid $gid") {
      log.trace(s"No-op chown: uid $uid, gid $gid -> $path")
      OK
    }

  // *************
  // File Handling
  // *************

  // To implement soft links in getattr set FileStat.S_IFLNK.
  // https://man7.org/linux/man-pages/man2/symlink.2.html
  //  override def symlink(oldpath: String, newpath: String): Int =
  // https://man7.org/linux/man-pages/man2/readlink.2.html
  //  override def readlink(path: String, buf: Pointer, size: Long): Int =

  override def create(path: String, mode: Long, fi: FuseFileInfo): Int =
    if settings.readonly then EROFS else fs(s"create $path") {
      val parts = backend.pathElements(path)
      if parts.length == 0 then ENOENT // Can't create root.
      else backend.entry(parts.dropRight(1)) match // Fetch parent entry.
        case None                => ENOENT  // Parent not known.
        case Some(_: FileEntry)  => ENOTDIR // Parent is a file.
        case Some(dir: DirEntry) =>
          backend.createAndOpen(dir.id, parts.last, now) match
            case None         => EEXIST // Entry with the given name already exists.
            case Some(handle) => // Yay, success!
              fi.fh.set(handle)
              OK
    }

  override def open(path: String, fi: FuseFileInfo): Int =
    fs(s"open $path") {
      backend.entry(path) match
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) => backend.open(file); fi.fh.set(file.id); OK
    }

  override def truncate(path: String, size: Long): Int =
    if settings.readonly then EROFS else fs(s"truncate $path .. $size") {
      backend.entry(path) match
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) => if (backend.truncate(file.id, size)) OK else EIO // false if called without create or open
    }

  override def write(path: String, source: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    if settings.readonly then EROFS else fs(s"write $path .. offset = $offset, size = $size") {
      val intSize = size.toInt.abs // We need to return an Int size, so here it is.
      def data: Iterator[(Long, Array[Byte])] = Iterator.range(0, intSize, memChunk).map { readOffset =>
        val chunkSize = math.min(memChunk, intSize - readOffset)
        offset + readOffset -> new Array[Byte](chunkSize).tap(source.get(readOffset, _, 0, chunkSize))
      }
      if offset < 0 || size != intSize then EOVERFLOW // With intSize being .abs (see above) checks for negative size, too.
      else if backend.write(fi.fh.get(), data) then intSize
      else EIO // false if called without create or open.
    }

  override def read(path: String, sink: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    fs(s"read $path .. offset = $offset, size = $size") {
      val intSize = size.toInt.abs // We need to return an Int size, so here it is.
      if offset < 0 || size != intSize then EOVERFLOW else // With intSize being .abs (see above) checks for negative size, too.
        val fileHandle = fi.fh.get()
        backend.read(fileHandle, offset, intSize) match
          case None => log.error(s"Read from '$path': No data for tree entry $fileHandle."); EIO
          case Some(data) => data.foldLeft(0) { case (size, position -> bytes) =>
            sink.put(position - offset, bytes, 0, bytes.length)
            size + bytes.length
          }
    }

  override def release(path: String, fi: FuseFileInfo): Int =
    fs(s"release $path") {
      val fileHandle = fi.fh.get()
      if backend.release(fileHandle) then OK else EIO // false if called without create or open
    }

  override def unlink(path: String): Int =
    if settings.readonly then EROFS else fs(s"unlink $path") {
      backend.entry(path) match
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) =>
          // From man unlink(2)
          // If the name was the last link to a file but any processes still have the file open,
          // the file will remain in existence until the last file descriptor referring to it is closed.
          // ... This means that the file handles need not be updated.
          if backend.deleteChildless(file) then OK
          else { log.warn(s"Can't delete regular file with children: $path"); ENOTEMPTY }
    }
