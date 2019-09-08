package dedup2

import java.io.File

import dedup2.Database._
import jnr.ffi.Pointer
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo}
import scala.util.chaining._

object Server extends App {
  val mountPoint = "J:\\"
  val repo = new File("").getAbsoluteFile
  val fs = new Server(repo)
  try fs.mount(java.nio.file.Paths.get(mountPoint), true, false)
  finally { fs.umount(); println(s"Repository unmounted from $mountPoint.") }
}

class Server(repo: File) extends FuseStubFS {
  private val db = new Database(H2.mem().tap(Database.initialize))
  private def sync[T](f: => T): T = synchronized(f)
  private val O777 = 511
  private def split(path: String): Array[String] =
    path.split("/").filter(_.nonEmpty)
  private def entry(path: String): Option[TreeEntry] =
    entry(split(path))
  private def entry(path: Array[String]): Option[TreeEntry] =
    path.foldLeft[Option[TreeEntry]](Some(Database.root)) {
      case (Some(DirEntry(id, _, _)), name) => db.child(id, name)
      case _ => None
    }

  override def umount(): Unit = sync {}

  /* Note: Calling FileStat.toString DOES NOT WORK, there's a PR: https://github.com/jnr/jnr-ffi/pull/176 */
  override def getattr(path: String, stat: FileStat): Int = sync {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(DirEntry(_, _, _)) =>
        stat.st_mode.set(FileStat.S_IFDIR | O777)
        stat.st_nlink.set(2)
        0
      case Some(FileEntry(_, _, _, lastModified, dataId)) =>
        stat.st_mode.set(FileStat.S_IFREG | O777)
        stat.st_nlink.set(1)
        stat.st_size.set(db.size(dataId))
        stat.st_mtim.tv_nsec.set((lastModified % 1000) * 1000)
        stat.st_mtim.tv_sec.set(lastModified / 1000)
        0
    }
  }

  /* Note: No benefit expected in implementing opendir/releasedir and handing over the file handle to readdir. */
  override def readdir(path: String, buf: Pointer, fill: FuseFillDir, offset: Long, fi: FuseFileInfo): Int = sync {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(FileEntry(_, _, _, _, _)) => -ErrorCodes.ENOTDIR
      case Some(DirEntry(id, _, _)) =>
        if (offset < 0 || offset.toInt != offset) -ErrorCodes.EOVERFLOW
        else {
          def names = Seq(".", "..") ++ db.children(id).map(_.name)
          // exists: side effect until a condition is met
          names.zipWithIndex.drop(offset.toInt).exists { case (name, k) => fill.apply(buf, name, null, k + 1) != 0 }
          0
        }
    }
  }

  override def rmdir(path: String): Int = sync {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(FileEntry(_, _, _, _, _)) => -ErrorCodes.ENOTDIR
      case Some(DirEntry(id, _, _)) => db.delete(id); 0
    }
  }.tap(r => println(s"rmdir $path -> $r"))

  // Renames a file. Other than the general contract of rename, newpath must not exist.
  override def rename(oldpath: String, newpath: String): Int = sync {
    val (oldParts, newParts) = (split(oldpath), split(newpath))
    if (oldParts.length == 0 || newParts.length == 0) -ErrorCodes.ENOENT
    else entry(oldParts) match {
      case None => -ErrorCodes.ENOENT
      case Some(source) =>
        entry(newParts.take(newParts.length - 1)) match {
          case None => -ErrorCodes.ENOENT
          case Some(_: FileEntry) => -ErrorCodes.ENOTDIR
          case Some(DirEntry(parentId, _, _)) =>
            val newName = newParts.last
            db.child(parentId, newName) match {
              case Some(_) => -ErrorCodes.EEXIST
              case None => db.moveRename(source.id, parentId, newName); 0
            }
        }
    }
  }.tap(r => println(s"rename $oldpath -> $newpath -> $r"))

  override def mkdir(path: String, mode: Long): Int = sync {
    val parts = split(path)
    if (parts.length == 0) -ErrorCodes.ENOENT
    else entry(parts.take(parts.length - 1)) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: FileEntry) => -ErrorCodes.ENOTDIR
      case Some(DirEntry(parentId, _, _)) =>
        val name = parts.last
        db.child(parentId, name) match {
          case Some(_) => -ErrorCodes.EEXIST
          case None => db.mkDir(parentId, name); 0
        }
    }
  }.tap(r => println(s"mkdir $path -> $r"))

}
