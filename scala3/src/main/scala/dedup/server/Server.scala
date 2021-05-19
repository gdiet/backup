package dedup
package server

import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.{Platform, Pointer}
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs, Timespec}
import ru.serce.jnrfuse.{FuseFillDir, FuseStubFS}

class Server(settings: Settings) extends FuseStubFS with util.ClassLogging {
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
            // TODO fill with k+1 or with k+1 - offset? -> Check with a LONG directory listing that logs if offset > 0
            // TODO For FileStat try to use S_IFREG / S_IFDIR directly - this might save some fs calls
            names.zipWithIndex.drop(offset.toInt).exists { case (name, k) => fill.apply(buf, name, null, k + 1 - offset) != 0 }
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
                    def copy(source: TreeEntry, newName: String, newParentId: Long): Boolean = source match {
                      case file: FileEntry =>
                        backend.copyFile(file, newParentId, newName)
                      case dir : DirEntry =>
                        backend.mkDir(newParentId, newName)
                          .exists(dirId => backend.children(source.id).forall(child => copy(child, child.name, dirId)))
                    }
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

  // *************
  // File Handling
  // *************

}
