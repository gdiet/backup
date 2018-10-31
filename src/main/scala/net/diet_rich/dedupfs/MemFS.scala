package net.diet_rich.dedupfs

import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.{Platform, Pointer}
import net.diet_rich.util._
import net.diet_rich.util.fs._
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs}
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}

class MemFS extends FuseStubFS with ClassLogging {
  val fs = new MemoryFS

  override def getattr(path: String, stat: FileStat): Int =
    log(s"getattr($path, fileStat)", asTrace = true)(fs.getattr(path, stat))

  override def mkdir(path: String, mode: Long): Int =
    log(s"mkdir($path, $mode)")(fs.mkdir(path, mode))

  override def readdir(path: String, buf: Pointer, filler: FuseFillDir, offset: Long, fi: FuseFileInfo): Int =
    log(s"readdir($path, buffer, filler, $offset, fileInfo)")(fs.readdir(path, buf, filler, offset, fi))

  override def rename(oldpath: String, newpath: String): Int =
    log(s"rename($oldpath, $newpath)")(fs.rename(oldpath, newpath))

  override def rmdir(path: String): Int =
    log(s"rmdir($path)")(fs.rmdir(path))

  override def statfs(path: String, stbuf: Statvfs): Int =
    log(s"statfs($path, buffer)")(fs.statfs(path, stbuf))

  override def unlink(path: String): Int =
    log(s"unlink($path)")(fs.unlink(path))

  override def create(path: String, mode: Long, fi: FuseFileInfo): Int =
    log(s"create($path, buf, $mode, fileInfo)")(fs.create(path, mode, fi))

  override def open(path: String, fi: FuseFileInfo): Int =
    log(s"open($path, fileInfo)")(fs.open(path, fi))

  override def write(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    log(s"write($path, buf, $size, $offset, fileInfo)")(fs.write(path, buf, size, offset, fi))

  override def truncate(path: String, size: Long): Int =
    log(s"truncate($path, $size)")(fs.truncate(path, size))

  override def read(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    log(s"read($path, buf, $size, $offset, fileInfo)")(fs.read(path, buf, size, offset, fi))
}

object MemFS extends ClassLogging with App {
  def mount(): AutoCloseable = {
    new AutoCloseable {
      val fuseFS = new MemFS
      try {
        // TODO Eventually, we want to have the mount point configurable.
        val mountPoint = Platform.getNativePlatform.getOS match {
          case WINDOWS => """I:\"""
          case _ => "/tmp/mntfs"
        }
        log.info(s"mount($mountPoint)")
        fuseFS.mount(java.nio.file.Paths.get(mountPoint), false, false)
      } catch { case e: Throwable =>
        log.info(e.getMessage)
        fuseFS.umount(); throw e
      }
      override def close(): Unit = fuseFS.umount()
    }
  }

  val fsHandle = mount()
  try io.StdIn.readLine("[enter] to exit ...")
  finally fsHandle.close()
}
