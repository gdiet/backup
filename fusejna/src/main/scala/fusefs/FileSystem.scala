package fusefs

import com.sun.jna.Pointer

import fusewrapper.Fuse
import fusewrapper.Fuse._
import fusewrapper.cconst.{Stat, Errno}
import fusewrapper.ctype.{Mode_t, Size_t, Off_t}
import fusewrapper.struct.{FuseFileInfoReference, StatReference, FuseWrapperOperations}

sealed trait Entry {
  def id: Long
  def name: String
}
case class Dir(id: Long, name: String) extends Entry
case class File(id: Long, name: String, size: Long) extends Entry

trait FileSystem {
  def entryFor(path: String): Option[Entry]
  def childrenOf(id: Long): Seq[Entry]
  def readFile(id: Long, offset: Long, size: Int): Array[Byte]
}

object FileSystem {

  def mount(fileSystem: FileSystem, mountDirectory: String): Int = {
    val ops: FuseWrapperOperations = new FuseWrapperOperations
    
    ops.readdir = new ReadDir {
      override def readDirectory(path: String, directoryBuffer: Pointer, fillerFunction: FuseFillDirT, ignoredOffset: Off_t, directoryInfo: StatReference): Int = {
        println(s"readdir $path with inode ${directoryInfo.st_ino}")
        fileSystem.entryFor(path) match {
          case Some(Dir(id, _)) =>
            // FIXME set inode?
            fillerFunction.invoke(directoryBuffer, ".", null, 0)
            fillerFunction.invoke(directoryBuffer, "..", null, 0)
            fileSystem.childrenOf(id).foreach(f =>
              fillerFunction.invoke(directoryBuffer, f.name, null, 0)
            )
            -Errno.OK
          case Some(File(_, _, _)) =>
            -Errno.ENOTDIR
          case None =>
            -Errno.ENOENT
        }
      }
    }
    
    ops.getattr = new GetAttr {
      override def getattr(path: String, fileInfo: StatReference): Int = {
        println(s"getattr $path with inode ${fileInfo.st_ino}")
        fileInfo.clear()
        fileSystem.entryFor(path) match {
          case Some(File(_, _, size)) =>
            // FIXME set inode to ID?
            fileInfo.st_mode = new Mode_t(Stat.S_IFREG | Integer.decode("0777"))
            fileInfo.st_size = new Off_t(size)
            -Errno.OK
          case Some(Dir(_, _)) =>
            fileInfo.st_mode = new Mode_t(Stat.S_IFDIR | Integer.decode("0444"))
            -Errno.OK
          case None =>
            -Errno.ENOENT
        }
      }
    }

    ops.read = new Read {
      override def read(path: String, data: Pointer, size: Size_t, offset: Off_t, fileInfo: FuseFileInfoReference): Int = {
        // FIXME use file handle (not inode) for ID?
        println(s"read $size at $offset from $path with handle ${fileInfo.fh}")
        fileSystem.entryFor(path) match {
          case Some(File(id, _, _)) =>
            // FIXME example code returns actual size read!
            val read = fileSystem.readFile(id, offset.longValue(), size.intValue())
            data.write(0, read, 0, size.intValue())
            -Errno.OK
          case Some(Dir(_, _)) =>
            -Errno.EISDIR
          case None =>
            -Errno.ENOENT
        }
      }
    }

    val fuseArgs = Array("fusewrapper", "-f", mountDirectory)
    Fuse.INSTANCE.fuse_main_real(fuseArgs.length, fuseArgs, ops, ops.size, Pointer.NULL)
  }
}
