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

  override def rmdir(path: String): Int =
    if settings.readonly then EROFS else watch("rmdir $path") {
      backend.entry(path) match
        case Some(dir: DirEntry) => if backend.children(dir.id).nonEmpty then ENOTEMPTY else { backend.delete(dir); OK }
        case Some(_)             => ENOTDIR
        case None                => ENOENT
    }

}
