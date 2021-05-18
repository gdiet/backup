package dedup
package server

import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.{Platform, Pointer}
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs, Timespec}
import ru.serce.jnrfuse.{FuseFillDir, FuseStubFS}
import scala.util.chaining._

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
      // TODO close level 1 store - there, warn about unclosed data entries and non-empty temp dir
      log.info(s"Dedup file system is stopped.")
      OK
    }

  /* Note: Calling FileStat.toString DOES NOT WORK, there's a PR: https://github.com/jnr/jnr-ffi/pull/176 */
  override def getattr(path: String, stat: FileStat): Int =
    watch(s"getattr $path") {
      def setCommon(time: Long, nlink: Int): Unit =
        stat.st_nlink.set(nlink)
        stat.st_mtim.tv_sec .set (time / 1000)
        stat.st_mtim.tv_nsec.set((time % 1000) * 1000000)
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

}
