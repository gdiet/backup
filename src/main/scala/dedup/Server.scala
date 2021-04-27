package dedup

import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.{Platform, Pointer}
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs, Timespec}
import ru.serce.jnrfuse.{FuseFillDir, FuseStubFS}

/*
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
class Server(settings: Settings) extends FuseStubFS with FuseConstants with ClassLogging {
  import settings.{copyWhenMoving, dataDir, readonly, temp}
  private val rights = if (readonly) 292 else 438 // o444 else o666
  private val store  = new Level1(settings)

  override protected def watch[T](msg: => String, logger: (=> String) => Unit = log.trace)(f: => T): T = {
    super.watch(msg, logger)(f).tap {
      case EIO       => log.error(s"EIO: $msg")
      case EINVAL    => log.warn (s"EINVAL: $msg")
      case EOVERFLOW => log.warn (s"EOVERFLOW: $msg")
      case _         => /**/
    }
  }

  override def umount(): Unit =
    watch(s"umount") {
      log.info(s"Stopping dedup file system...")
      super.umount()
      store.close()
      if (DataEntry.openEntries != 0) log.warn(s"${DataEntry.openEntries} data entries have not been closed.")
      if (!readonly)
        if (temp.listFiles().nonEmpty) log.warn(s"Temp dir is not empty: $temp")
        else temp.delete()
      log.info(s"Dedup file system is stopped.")
      OK
    }

  /* Note: Calling FileStat.toString DOES NOT WORK, there's a PR: https://github.com/jnr/jnr-ffi/pull/176 */
  override def getattr(path: String, stat: FileStat): Int =
    watch(s"getattr $path") {
      def setCommon(time: Long, nlink: Int): Unit = {
        stat.st_nlink.set(nlink)
        stat.st_mtim.tv_sec .set (time / 1000)
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
          stat.st_size.set(store.size(file.id, file.dataId))
          OK
      }
    }

  // see man UTIMENSAT(2)
  override def utimens(path: String, timespec: Array[Timespec]): Int =
    if (readonly) EROFS else watch(s"utimens $path") {
      if (timespec.length < 2) EIO
      else {
        val sec = timespec(1).tv_sec .get
        val nan = timespec(1).tv_nsec.longValue
        if (sec < 0 || nan < 0 || nan > 1000000000) EINVAL
        else store.entry(path) match {
          case None => ENOENT
          case Some(entry) => store.setTime(entry.id, sec*1000 + nan/1000000); OK
        }
      }
    }

  // Note: No benefit expected in implementing opendir/releasedir and handing over the file handle to readdir.
  override def readdir(path: String, buf: Pointer, fill: FuseFillDir, offset: Long, fi: FuseFileInfo): Int =
    watch(s"readdir $path $offset") {
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

  override def rmdir(path: String): Int =
    if (readonly) EROFS else watch("rmdir $path") {
      store.entry(path) match {
        case Some(dir: DirEntry) => if (store.children(dir.id).nonEmpty) ENOTEMPTY else { store.delete(dir); OK }
        case Some(_)             => ENOTDIR
        case None                => ENOENT
      }
    }

  // If copyWhenMoving is active, the last persisted state of files is copied - without the current modifications.
  override def rename(oldpath: String, newpath: String): Int =
    if (readonly) EROFS else watch(s"rename $oldpath .. $newpath") {
      val (oldParts, newParts) = (store.split(oldpath), store.split(newpath))
      if (oldParts.length == 0 || newParts.length == 0) ENOENT
      else store.entry(oldParts) match {
        case None => ENOENT
        case Some(origin) => store.entry(newParts.take(newParts.length - 1)) match {
          case None => ENOENT
          case Some(_  : FileEntry) => ENOTDIR
          case Some(dir: DirEntry ) =>
            val newName = newParts.last
            origin -> store.child(dir.id, newName) match {
              case (_: FileEntry) -> Some(_: DirEntry) => EISDIR
              case _ -> previous =>
                def copy(origin: TreeEntry, newName: String, newParentId: Long): Boolean = origin match {
                  case file: FileEntry =>
                    store.copyFile(file, newParentId, newName)
                  case dir : DirEntry =>
                    store.mkDir(newParentId, newName)
                      .exists(dirId => store.children(dir.id).forall(child => copy(child, child.name, dirId)))
                }
                // Other than the contract of rename (see https://linux.die.net/man/2/rename), the
                // replace operation is not atomic. This is tolerated in order to simplify the code.
                previous.foreach(store.delete)
                if (origin.parentId != dir.id && copyWhenMoving.get()) {
                  if (copy(origin, newName, dir.id)) OK else EEXIST
                } else { store.update(origin.id, dir.id, newName); OK }
            }
        }
      }
    }

  override def mkdir(path: String, mode: Long): Int =
    if (readonly) EROFS else watch(s"mkdir $path") {
      val parts = store.split(path)
      if (parts.length == 0) ENOENT
      else store.entry(parts.take(parts.length - 1)) match {
        case None => ENOENT
        case Some(_: FileEntry) => ENOTDIR
        case Some(dir: DirEntry) =>
          val name = parts.last
          store.mkDir(dir.id, name).fold(EEXIST)(_ => OK)
      }
    }

  // statfs needs to be implemented on Windows in order to allow for copying data from
  // other devices because winfsp calculates the volume size based on the statvfs call.
  // see ru.serce.jnrfuse.examples.MemoryFS.statfs and
  // https://github.com/billziss-gh/winfsp/blob/14e6b402fe3360fdebcc78868de8df27622b565f/src/dll/fuse/fuse_intf.c#L654
  // On Linux more of the stbuf struct would need to be filled to get sensible disk space values.
  override def statfs(path: String, stbuf: Statvfs): Int =
    watch(s"statfs $path") {
      if (Platform.getNativePlatform.getOS == WINDOWS) {
        stbuf.f_frsize.set(                        32768) // fs block size
        stbuf.f_bfree .set(dataDir.getFreeSpace  / 32768) // free blocks in fs
        stbuf.f_blocks.set(dataDir.getTotalSpace / 32768) // total data blocks in file system
      }
      OK
    }

  // #########
  // # Files #
  // #########

  // When implementing soft links, need to extend getattr, set FileStat.S_IFLNK

  //  // https://man7.org/linux/man-pages/man2/symlink.2.html
  //  override def symlink(oldpath: String, newpath: String): Int =
  //    if (readonly) EROFS else guard(s"symlink $oldpath -> $newpath") {
  //      log.info(s"symlink $oldpath -> $newpath")
  //      val parts = store.split(newpath)
  //      if (parts.length == 0) ENOENT // can't create root
  //      else store.entry(parts.dropRight(1)) match { // fetch parent entry
  //        case None => ENOENT // parent not known
  //        case Some(_: FileEntry)  => ENOTDIR // parent is a file
  //        case Some(dir: DirEntry) =>
  //          val name = parts.last
  //          store.child(dir.id, name) match {
  //            case Some(_) => EEXIST // entry with the given name already exists
  //            case None =>
  //              val id = store.createAndOpen(dir.id, name, now)
  //              if (!store.write(id, 0, s"Symlink: $oldpath".getBytes("UTF-8")))
  //                log.warn(s"Could not write symlink.")
  //              if (!store.release(id))
  //                log.warn(s"Could not release symlink.")
  //              OK
  //          }
  //      }
  //    }

  //  // https://man7.org/linux/man-pages/man2/readlink.2.html
  //  override def readlink(path: String, buf: Pointer, size: Long): Int =
  //    guard(s"readlink $path size $size") {
  //      log.info(s"readlink $path size $size")
  //      0 // Number of bytes placed in buf.
  //    }

  override def create(path: String, mode: Long, fi: FuseFileInfo): Int =
    if (readonly) EROFS else watch(s"create $path") {
      val parts = store.split(path)
      if (parts.length == 0) ENOENT // can't create root
      else store.entry(parts.dropRight(1)) match { // fetch parent entry
        case None => ENOENT // parent not known
        case Some(_: FileEntry)  => ENOTDIR // parent is a file
        case Some(dir: DirEntry) =>
          val name = parts.last
          store.createAndOpen(dir.id, name, now) match {
            case None => EEXIST // entry with the given name already exists
            case Some(handle) => fi.fh.set(handle); OK
          }
      }
    }

  override def open(path: String, fi: FuseFileInfo): Int =
    watch(s"open $path") {
      store.entry(path) match {
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) => store.open(file); fi.fh.set(file.id); OK
      }
    }

  override def release(path: String, fi: FuseFileInfo): Int =
    watch(s"release $path") {
      val fileHandle = fi.fh.get()
      if (store.release(fileHandle)) OK else EIO // false if called without create or open
    }

  override def write(path: String, source: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    if (readonly) EROFS else watch(s"write $path .. offset = $offset, size = $size") {
      val intSize = size.toInt.abs // We need to return an Int size, so here it is.
      def data: LazyList[(Long, Array[Byte])] = LazyList.range(0, intSize, memChunk).map { readOffset =>
        def chunkSize = math.min(memChunk, intSize - readOffset)
        offset -> new Array[Byte](chunkSize).tap(source.get(readOffset, _, 0, chunkSize))
      }
      if (offset < 0 || size != intSize) EOVERFLOW // With intSize being .abs (see above) checks for negative size, too.
      else if (store.write(fi.fh.get(), data)) intSize
      else EIO // false if called without create or open.
    }

  override def truncate(path: String, size: Long): Int =
    if (readonly) EROFS else watch(s"truncate $path .. $size") {
      store.entry(path) match {
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) =>
          if (store.truncate(file.id, size)) OK else EIO // false if called without create or open
      }
    }

  override def read(path: String, sink: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    watch(s"read $path .. offset = $offset, size = $size") {
      val intSize = size.toInt.abs // We need to return an Int size, so here it is.
      if (offset < 0 || size != intSize) EOVERFLOW else { // With intSize being .abs (see above) checks for negative size, too.
        val fileHandle = fi.fh.get()
        store
          .read(fileHandle, offset, intSize, sink).map(_.toInt)
          .getOrElse { log.warn(s"read - no data for tree entry $fileHandle (path is $path)"); ENOENT }
      }
    }

  override def unlink(path: String): Int = if (readonly) EROFS else
    watch(s"unlink $path") {
      store.entry(path) match {
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) => store.delete(file); OK
      }
    }
}
