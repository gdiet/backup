package _tryout.e03

import jnr.ffi.Pointer
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
  val fs = new MemoryFS()

  override def create(path: String, mode: Long, fi: FuseFileInfo): Int = 
    fs.create(path, mode, fi).tap(r => println(s"create $path -> $r"))

  override def getattr(path: String, stat: FileStat): Int = 
    fs.getattr(path, stat).tap(r => println(s"getattr $path -> $r"))

  override def mkdir(path: String, mode: Long): Int = 
    fs.mkdir(path, mode).tap(r => println(s"mkdir $path -> $r"))

  override def read(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = 
    fs.read(path, buf, size, offset, fi).tap(r => println(s"read $path $offset:$size -> $r"))

  override def readdir(path: String, buf: Pointer, filter: FuseFillDir, offset: Long, fi: FuseFileInfo): Int = 
    fs.readdir(path, buf, filter, offset, fi).tap(r => println(s"readdir $path $offset -> $r"))

  override def statfs(path: String, stbuf: Statvfs): Int = 
    fs.statfs(path, stbuf).tap(r => println(s"statfs $path -> $r"))

  override def rename(oldpath: String, newpath: String): Int = 
    fs.rename(oldpath, newpath).tap(r => println(s"rename $oldpath $newpath -> $r"))

  override def rmdir(path: String): Int = 
    fs.rmdir(path).tap(r => println(s"rmdir $path -> $r"))

  override def truncate(path: String, size: Long): Int = 
    fs.truncate(path, size).tap(r => println(s"truncate $path -> $r"))

  override def unlink(path: String): Int = 
    fs.unlink(path).tap(r => println(s"unlink $path -> $r"))

  override def open(path: String, fi: FuseFileInfo): Int = 
    fs.open(path, fi).tap(r => println(s"open $path -> $r"))

  override def write(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = 
    fs.write(path, buf, size, offset, fi).tap(r => println(s"write $path $offset:$size -> $r"))
}
