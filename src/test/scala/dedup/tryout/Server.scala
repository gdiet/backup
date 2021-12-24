package dedup.tryout

import jnr.ffi.Pointer
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs}

@main def tryout(): Unit =
  val fs = new Server
  try
    println("Mount...")
    fs.mount(java.io.File("J:\\").toPath, true, false, Array("-o", "volname=DedupFS"))
    println("Finished.")
  catch case e =>
    println(e)
    fs.umount()

class Server extends FuseStubFS:
  private val O777 = 511
  private val helloPath = "/hello.txt"
  private val content = "Hello World!".getBytes("UTF-8")

  override def getattr(path: String, stat: FileStat): Int =
    val result = if (path == "/") {
      stat.st_mode.set(FileStat.S_IFDIR | 438)//O777)
      stat.st_nlink.set(2)
      0
    } else if (path == helloPath) {
      stat.st_mode.set(FileStat.S_IFREG | O777)
      stat.st_nlink.set(1)
      stat.st_size.set(content.length)
      0
    } else -ErrorCodes.ENOENT
    println(s"getattr $path => $result")
    result

//  /* Note: No benefit expected in implementing opendir/releasedir and handing over the file handle to readdir.
//   * Note: offset is not handled (but also it not necessary in this simple case). */
//  override def readdir(path: String, buf: Pointer, fill: FuseFillDir, offset: Long, fi: FuseFileInfo): Int =
//    val result = if (path == "/") {
//      fill(buf, ".", null, 0)
//      fill(buf, "..", null, 0)
//      fill(buf, helloPath.drop(1), null, 0)
//      0
//    } else -ErrorCodes.ENOENT
//    println(s"readdir $path => $result")
//    result
//
//  override def open(path: String, fi: FuseFileInfo): Int =
//    val result = if (path == helloPath) 0 else -ErrorCodes.ENOENT
//    println(s"open $path => $result")
//    result
//
//  override def read(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = {
//    val result = if (path == helloPath) {
//      if (offset >= content.size) 0
//      else {
//        val bytesRead = math.min(content.size - offset, size).toInt
//        buf.put(0, content, offset.toInt, bytesRead)
//        bytesRead
//      }
//    } else -ErrorCodes.ENOENT
//    println(s"read $path => $result")
//    result
//  }
