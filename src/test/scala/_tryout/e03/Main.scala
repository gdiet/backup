package _tryout.e03

import jnr.ffi.Pointer
import org.slf4j.LoggerFactory
import ru.serce.jnrfuse.examples.MemoryFS
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs}
import ru.serce.jnrfuse.{FuseFillDir, FuseStubFS}

import scala.util.chaining._

object Main extends App {
  val mountPoint = "J:\\"
  val fs = new Server()
  try fs.mount(java.nio.file.Paths.get(mountPoint), true, false)
  finally { fs.umount(); println(s"Repository unmounted from $mountPoint.") }
}

class Server extends FuseStubFS {
  private val log = LoggerFactory.getLogger(getClass)
  
  val fs = new MemoryFS()

  override def create(path: String, mode: Long, fi: FuseFileInfo): Int = 
    fs.create(path, mode, fi).tap(r => log.info(s"create $path -> $r"))

  override def getattr(path: String, stat: FileStat): Int = 
    fs.getattr(path, stat).tap(r => log.info(s"getattr $path -> $r"))

  override def mkdir(path: String, mode: Long): Int = 
    fs.mkdir(path, mode).tap(r => log.info(s"mkdir $path -> $r"))

  override def read(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = 
    fs.read(path, buf, size, offset, fi).tap(r => log.info(s"read $path $offset:$size -> $r"))

  override def readdir(path: String, buf: Pointer, filter: FuseFillDir, offset: Long, fi: FuseFileInfo): Int = 
    fs.readdir(path, buf, filter, offset, fi).tap(r => log.info(s"readdir $path $offset -> $r"))

  override def statfs(path: String, stbuf: Statvfs): Int = 
    fs.statfs(path, stbuf).tap(r => log.info(s"statfs $path -> $r"))

  override def rename(oldpath: String, newpath: String): Int = 
    fs.rename(oldpath, newpath).tap(r => log.info(s"rename $oldpath $newpath -> $r"))

  override def rmdir(path: String): Int = 
    fs.rmdir(path).tap(r => log.info(s"rmdir $path -> $r"))

  override def truncate(path: String, size: Long): Int = 
    fs.truncate(path, size).tap(r => log.info(s"truncate $path -> $r"))

  override def unlink(path: String): Int = 
    fs.unlink(path).tap(r => log.info(s"unlink $path -> $r"))

  override def open(path: String, fi: FuseFileInfo): Int = 
    fs.open(path, fi).tap(r => log.info(s"open $path -> $r"))

  override def write(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = 
    fs.write(path, buf, size, offset, fi).tap(r => log.info(s"write $path $offset:$size -> $r"))
}
