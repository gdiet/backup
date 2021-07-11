package dedup
package server

import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.{Platform, Pointer}
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs, Timespec}
import ru.serce.jnrfuse.{FuseFillDir, FuseStubFS}
import java.util.concurrent.atomic.AtomicBoolean

class Server(settings: Settings) extends FuseStubFS with util.ClassLogging:

  private val rights = if (settings.readonly) 292 else 438 // o444 else o666
  private val backend = Level1(settings)

  override protected def watch[T](msg: => String, logger: (=> String) => Unit = log.trace)(f: => T): T =
    super.watch(msg, logger)(f).tap {
      case EIO       => log.error(s"EIO: $msg")
      case EINVAL    => log.warn (s"EINVAL: $msg")
      case EOVERFLOW => log.warn (s"EOVERFLOW: $msg")
      case _         => /**/
    }

  override def umount(): Unit =
    watch(s"umount") {
      log.info(s"Stopping dedup file system...")
      super.umount()
      log.info(s"Dedup file system is stopped.")
      backend.close()
      OK
    }

  // *************
  // Tree Handling
  // *************

  /* Note: Calling FileStat.toString DOES NOT WORK, there's a PR: https://github.com/jnr/jnr-ffi/pull/176 */
  override def getattr(path: String, stat: FileStat): Int =
    watch(s"getattr $path") {
      def setCommon(time: Time, nlink: Int): Unit =
        stat.st_nlink.set(nlink)
        stat.st_mtim.tv_sec .set (time.toLong / 1000)
        stat.st_mtim.tv_nsec.set((time.toLong % 1000) * 1000000)
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
          stat.st_size.set(backend.size(file.id, file.dataId))
          OK
    }

  override def mkdir(path: String, mode: Long): Int =
    if settings.readonly then EROFS else watch(s"mkdir $path") {
      val parts = backend.split(path)
      if parts.length == 0 then ENOENT else
        backend.entry(parts.dropRight(1)) match
          case None                 => ENOENT
          case Some(_  : FileEntry) => ENOTDIR
          case Some(dir: DirEntry ) => backend.mkDir(dir.id, parts.last).fold(EEXIST)(_ => OK)
    }

  // No benefit expected from implementing opendir/releasedir and handing over a file handle to readdir.
  override def readdir(path: String, buf: Pointer, fill: FuseFillDir, offset: Long, fi: FuseFileInfo): Int =
    watch(s"readdir $path $offset") {
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
    if (settings.readonly) EROFS else watch(s"rename $oldpath .. $newpath") {
      val oldParts = backend.split(oldpath)
      val newParts = backend.split(newpath)
      if oldParts.length == 0 || newParts.length == 0 then ENOENT else
      if oldParts.sameElements(newParts)              then OK     else
        backend.entry(oldParts) match
          case None         => ENOENT // Oldpath does not exist.
          case Some(origin) => backend.entry(newParts.dropRight(1)) match
            case None                       => ENOENT  // Parent of newpath does not exist.
            case Some(_        : FileEntry) => ENOTDIR // Parent of newpath is a file.
            case Some(targetDir: DirEntry ) =>
              val newName = newParts.last
              origin -> backend.child(targetDir.id, newName) match
                case (_: FileEntry) -> Some(_: DirEntry) => EISDIR // Oldpath is a file and newpath is a dir.
                case  _             -> previous          =>
                  // Other than the contract of rename (see https://linux.die.net/man/2/rename), the
                  // replace operation is not atomic. This is tolerated in order to simplify the code.
                  previous.foreach(backend.delete)
                  if origin.parentId != targetDir.id && settings.copyWhenMoving.get() then
                    def copy(source: TreeEntry, newName: String, newParentId: Long): Boolean = source match
                      case file: FileEntry =>
                        backend.copyFile(file, newParentId, newName)
                      case dir : DirEntry =>
                        backend.mkDir(newParentId, newName)
                          .exists(dirId => backend.children(source.id).forall(child => copy(child, child.name, dirId)))
                    if (copy(origin, newName, targetDir.id)) OK else EEXIST
                  else
                    backend.update(origin.id, targetDir.id, newName)
                    OK
    }

  override def rmdir(path: String): Int =
    if settings.readonly then EROFS else watch("rmdir $path") {
      backend.entry(path) match
        case Some(dir: DirEntry) => if backend.children(dir.id).nonEmpty then ENOTEMPTY else { backend.delete(dir); OK }
        case Some(_)             => ENOTDIR
        case None                => ENOENT
    }

  // Implemented for Windows in order to allow for copying data from other devices because winfsp calculates
  // volume size based on statvfs. See ru.serce.jnrfuse.examples.MemoryFS.statfs and
  // https://github.com/billziss-gh/winfsp/blob/14e6b402fe3360fdebcc78868de8df27622b565f/src/dll/fuse/fuse_intf.c#L654
  // On Linux more of the stbuf struct would need to be filled to get sensible disk space values.
  override def statfs(path: String, stbuf: Statvfs): Int =
    watch(s"statfs $path") {
      if Platform.getNativePlatform.getOS == WINDOWS then
        stbuf.f_frsize.set(                                 32768) // fs block size
        stbuf.f_bfree .set(settings.dataDir.getFreeSpace  / 32768) // free blocks in fs
        stbuf.f_blocks.set(settings.dataDir.getTotalSpace / 32768) // total data blocks in file system
      OK
    }

  // see man UTIMENSAT(2)
  override def utimens(path: String, timespec: Array[Timespec]): Int =
    if settings.readonly then EROFS else watch(s"utimens $path") {
      if timespec.length < 2 then EIO else
        val sec = timespec(1).tv_sec .get
        val nan = timespec(1).tv_nsec.longValue
        if sec < 0 || nan < 0 || nan > 1000000000 then EINVAL else
          backend.entry(path) match
            case None        => ENOENT
            case Some(entry) => backend.setTime(entry.id, sec*1000 + nan/1000000); OK
    }

  private val logChmod = AtomicBoolean(true)
  override def chmod(path: String, mode: Long): Int =
    watch(s"chmod $path $mode") {
      if logChmod.getAndSet(false) then
        log.info(s"no-op chmod provided for certain Linux file managers.")
      OK
    }

  private val logChown = AtomicBoolean(true)
  override def chown(path: String, uid: Long, gid: Long): Int =
    watch(s"chown $path $uid $gid") {
      if logChown.getAndSet(false) then
        log.info(s"no-op chown provided for certain Linux file managers.")
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
    if settings.readonly then EROFS else watch(s"create $path") {
      val parts = backend.split(path)
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
    watch(s"open $path") {
      backend.entry(path) match
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) => backend.open(file); fi.fh.set(file.id); OK
    }

  override def truncate(path: String, size: Long): Int =
    if settings.readonly then EROFS else watch(s"truncate $path .. $size") {
      backend.entry(path) match
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) => if (backend.truncate(file.id, size)) OK else EIO // false if called without create or open
    }

  override def write(path: String, source: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    if settings.readonly then EROFS else watch(s"write $path .. offset = $offset, size = $size") {
      val intSize = size.toInt.abs // We need to return an Int size, so here it is.
      if offset < 0 || size != intSize then EOVERFLOW // With intSize being .abs (see above) checks for negative size, too.
      def data: LazyList[(Long, Array[Byte])] = LazyList.range(0, intSize, memChunk).map { readOffset =>
        val chunkSize = math.min(memChunk, intSize - readOffset)
        offset + readOffset -> new Array[Byte](chunkSize).tap(source.get(readOffset, _, 0, chunkSize))
      }
      else if backend.write(fi.fh.get(), data) then intSize
      else EIO // false if called without create or open.
    }

  override def read(path: String, sink: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    watch(s"read $path .. offset = $offset, size = $size") {
      val intSize = size.toInt.abs // We need to return an Int size, so here it is.
      if offset < 0 || size != intSize then EOVERFLOW else // With intSize being .abs (see above) checks for negative size, too.
        val fileHandle = fi.fh.get()
        backend
          .read(fileHandle, offset, intSize, sink).map(_.toInt)
          .getOrElse { log.warn(s"read - no data for tree entry $fileHandle (path is $path)"); ENOENT }
    }

  override def release(path: String, fi: FuseFileInfo): Int =
    watch(s"release $path") {
      val fileHandle = fi.fh.get()
      if backend.release(fileHandle) then OK else EIO // false if called without create or open
    }

  override def unlink(path: String): Int =
    if settings.readonly then EROFS else watch(s"unlink $path") {
      backend.entry(path) match
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) => backend.delete(file); OK
    }
