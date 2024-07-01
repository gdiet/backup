package dedup.tryout2

import jnr.ffi.Platform.OS
import jnr.ffi.{Platform, Pointer}
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo}
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}

import java.nio.file.Paths

object MinimalFS extends App {
  val mountPoint = if (Platform.getNativePlatform.getOS == OS.WINDOWS) "J:\\" else "/tmp/mnth"
  val fs = new MinimalFS()
  try fs.mount(Paths.get(mountPoint), true, true)
  finally fs.umount()
}

class MinimalFS extends FuseStubFS {
  private val O777 = 511
  private val helloPath = "/hello.txt"
  private val content = "Hello World!".getBytes("UTF-8")

  override def getattr(path: String, stat: FileStat): Int =
    if (path == "/") {
      stat.st_mode.set(FileStat.S_IFDIR | O777)
      stat.st_nlink.set(2)
      0
    } else if (path == helloPath) {
      stat.st_mode.set(FileStat.S_IFREG | O777)
      stat.st_nlink.set(1)
      stat.st_size.set(content.length)
      0
    } else -ErrorCodes.ENOENT

  /* Note: No benefit expected in implementing opendir/releasedir and handing over the file handle to readdir.
   * Note: offset is not handled (but also it not necessary in this simple case). */
  override def readdir(path: String, buf: Pointer, fill: FuseFillDir, offset: Long, fi: FuseFileInfo): Int =
    if (path == "/") {
      fill(buf, ".", null, 0)
      fill(buf, "..", null, 0)
      fill(buf, helloPath.drop(1), null, 0)
      0
    } else -ErrorCodes.ENOENT

  override def open(path: String, fi: FuseFileInfo): Int =
    if (path == helloPath) 0 else -ErrorCodes.ENOENT

  override def read(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = {
    if (path == helloPath) {
      if (offset >= content.size) 0
      else {
        val bytesRead = math.min(content.size - offset, size).toInt
        buf.put(0, content, offset.toInt, bytesRead)
        bytesRead
      }
    } else -ErrorCodes.ENOENT
  }
}
