package dedup
package server

import dedup.backend.{Backend, FileSystemReadOnly, ReadBackend, WriteBackend}
import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.{Platform, Pointer}
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs, Timespec}
import ru.serce.jnrfuse.{FuseFillDir, FuseStubFS}

import java.util.concurrent.atomic.AtomicBoolean

object ServerRead:
  def apply(settings: Settings): ServerRead =
    new ServerRead(settings, ReadBackend(settings))

class ServerRead(settings: Settings, backend: ReadBackend) extends FuseStubFS with util.ClassLogging:

  // Windows needs the executable flag, at least for the root. 356 = o555
  // On Linux, clear the executable flag because it is rather dangerous. 292 = o444
  protected val rights: Int = if Platform.getNativePlatform.getOS == WINDOWS then 365 else 292

  /** Utility wrapper for fuse server file system methods. */
  protected def fs(msg: => String, logger: (=> String) => Unit = log.trace)(f: => Int): Int =
    watch(msg, logger)(
      try f catch case _: FileSystemReadOnly => EROFS
    ).tap {
      case EIO => log.error(s"EIO: $msg")
      case EINVAL => log.warn(s"EINVAL: $msg")
      case EOVERFLOW => log.warn(s"EOVERFLOW: $msg")
      case _ => /**/
    }

  override def umount(): Unit = fs(s"umount") {
    log.info(s"Stopping dedup file system...")
    super.umount()
    log.info(s"Dedup file system is stopped.")
    backend.shutdown()
    OK
  }

  // *************
  // Tree Handling
  // *************

  override def getattr(path: String, stat: FileStat): Int = fs(s"getattr $path") {
    def setCommon(time: Time, nlink: Int): Unit =
      stat.st_nlink.set(nlink)
      stat.st_mtim.tv_sec.set(time.asLong / 1000)
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

  // No benefit expected from implementing opendir/releasedir and handing over a file handle to readdir.
  override def readdir(path: String, buf: Pointer, fill: FuseFillDir, offset: Long, fi: FuseFileInfo): Int = fs(s"readdir $path $offset") {
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
      case None => ENOENT
  }

  // Implemented for Windows in order to allow for copying data from other devices because winfsp calculates
  // volume size based on statvfs. See ru.serce.jnrfuse.examples.MemoryFS.statfs and
  // https://github.com/billziss-gh/winfsp/blob/14e6b402fe3360fdebcc78868de8df27622b565f/src/dll/fuse/fuse_intf.c#L654
  // On Linux more of the stbuf struct would need to be filled to get sensible disk space values.
  override def statfs(path: String, stbuf: Statvfs): Int = fs(s"statfs $path") {
    if Platform.getNativePlatform.getOS == WINDOWS then
      stbuf.f_frsize.set(32768) // fs block size
      stbuf.f_bfree.set(settings.dataDir.getFreeSpace / 32768) // free blocks in fs
      stbuf.f_blocks.set(settings.dataDir.getTotalSpace / 32768) // total data blocks in file system
    OK
  }

  // *************
  // File Handling
  // *************

  // To implement soft links in getattr set FileStat.S_IFLNK.
  // https://man7.org/linux/man-pages/man2/readlink.2.html
  // override def readlink(path: String, buf: Pointer, size: Long): Int =
  // And on the write side:
  // https://man7.org/linux/man-pages/man2/symlink.2.html
  // override def symlink(oldpath: String, newpath: String): Int =

  override def open(path: String, fi: FuseFileInfo): Int = fs(s"open $path") {
    backend.entry(path) match
      case None => ENOENT
      case Some(_: DirEntry) => EISDIR
      case Some(file: FileEntry) => backend.open(file.id, file.dataId); fi.fh.set(file.id); OK
  }

  override def read(path: String, sink: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = fs(s"read $path .. offset = $offset, size = $size") {
    val intSize = size.toInt.abs // We need to return an Int size, so here it is.
    if offset < 0 || size != intSize then EOVERFLOW else // With intSize being .abs (see above) checks for negative size, too.
      val fileHandle = fi.fh.get()
      backend.read(fileHandle, offset, intSize) match
        case None => log.error(s"Read from '$path': No data for tree entry $fileHandle."); EIO
        case Some(chunks) =>
          chunks.map { case (position, data) =>
            sink.write(position - offset, data)
            data.length
          }.sum
  }

  override def release(path: String, fi: FuseFileInfo): Int = fs(s"release $path") {
    val fileHandle = fi.fh.get()
    if backend.release(fileHandle).isDefined then OK else EIO // None if called without create or open.
  }
