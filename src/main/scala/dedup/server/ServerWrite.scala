package dedup
package server

import dedup.backend.{Backend, FileSystemReadOnly, ReadBackend, WriteBackend}
import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.{Platform, Pointer}
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs, Timespec}
import ru.serce.jnrfuse.{FuseFillDir, FuseStubFS}

import java.util.concurrent.atomic.AtomicBoolean

object ServerWrite:
  def apply(settings: Settings): ServerWrite =
    new ServerWrite(settings, WriteBackend(settings))

class ServerWrite(settings: Settings, backend: WriteBackend) extends ServerRead(settings, backend):

  // Windows needs the executable flag, at least for the root. 511 = o777
  // On Linux, clear the executable flag because it is rather dangerous. 438 = o666
  override val rights: Int = if Platform.getNativePlatform.getOS == WINDOWS then 511 else 438

  // *************
  // Tree Handling
  // *************

  override def mkdir(path: String, mode: Long): Int = fs(s"mkdir $path") {
    val parts = backend.split(path)
    if parts.length == 0 then ENOENT else
      backend.entry(parts.dropRight(1)) match
        case None => ENOENT
        case Some(_: FileEntry) => ENOTDIR
        case Some(dir: DirEntry) => backend.mkDir(dir.id, parts.last).fold(EEXIST)(_ => OK)
  }

  // If copyWhenMoving is active, the last persisted state of files is copied - without any current modifications.
  override def rename(oldpath: String, newpath: String): Int = fs(s"rename $oldpath .. $newpath") {
    val oldParts = backend.split(oldpath)
    val newParts = backend.split(newpath)
    if oldParts.length == 0 || newParts.length == 0 then ENOENT else if oldParts.sameElements(newParts) then OK else
      backend.entry(oldParts) match
        case None => ENOENT // oldpath does not exist.
        case Some(origin) => backend.entry(newParts.dropRight(1)) match
          case None => ENOENT // Parent of newpath does not exist.
          case Some(_: FileEntry) => ENOTDIR // Parent of newpath is a file.
          case Some(targetDir: DirEntry) =>
            val newName = newParts.last
            origin -> backend.child(targetDir.id, newName) match
              case (_: FileEntry, Some(_: DirEntry)) => EISDIR // oldpath is a file and newpath is a dir.
              case (_, previous) =>
                // Other than the contract of rename (see https://linux.die.net/man/2/rename), the
                // replace operation is not atomic. This is tolerated in order to simplify the code.
                if !previous.forall(backend.deleteChildless) then ENOTEMPTY
                else if origin.parentId != targetDir.id && settings.copyWhenMoving.get() then
                  def copy(source: TreeEntry, newName: String, newParentId: Long): Boolean = source match
                    case file: FileEntry =>
                      backend.copyFile(file, newParentId, newName)
                    case dir: DirEntry =>
                      backend.mkDir(newParentId, newName)
                        .exists(dirId => backend.children(dir.id).forall(child => copy(child, child.name, dirId)))

                  if (copy(origin, newName, targetDir.id)) OK else EEXIST
                else
                  if backend.renameMove(origin.id, targetDir.id, newName) then OK else EEXIST
  }

  override def rmdir(path: String): Int = fs("rmdir $path") {
    backend.entry(path) match
      case Some(dir: DirEntry) => if backend.deleteChildless(dir) then OK else ENOTEMPTY
      case Some(_)             => ENOTDIR
      case None                => ENOENT
  }

  // see man UTIMENSAT(2)
  override def utimens(path: String, timespec: Array[Timespec]): Int = fs(s"utimens $path") {
    if timespec.length < 2 then EIO else
      val sec = timespec(1).tv_sec .get
      val nan = timespec(1).tv_nsec.longValue
      if sec < 0 || nan < 0 || nan > 1000000000 then EINVAL else
        backend.entry(path) match
          case None        => ENOENT
          case Some(entry) => backend.setTime(entry.id, sec*1000 + nan/1000000); OK
  }

  override def chmod(path: String, mode: Long): Int =
    fs(s"chmod $path $mode") {
      log.debug(s"No-op chmod: $mode -> $path")
      OK
    }

  override def chown(path: String, uid: Long, gid: Long): Int =
    fs(s"chown $path $uid $gid") {
      log.debug(s"No-op chown: uid $uid, gid $gid -> $path")
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

  override def create(path: String, mode: Long, fi: FuseFileInfo): Int = fs(s"create $path") {
    val parts = backend.split(path)
    if parts.length == 0 then ENOENT // Can't create root.
    else backend.entry(parts.dropRight(1)) match // Fetch parent entry.
      case None                => ENOENT  // Parent not known.
      case Some(_: FileEntry)  => ENOTDIR // Parent is a file.
      case Some(dir: DirEntry) =>
        backend.createAndOpen(dir.id, parts.last, now) match
          case None         => EEXIST // Entry with the given name already exists.
          case Some(fileId) => // Yay, success!
            fi.fh.set(fileId)
            OK
  }

  override def truncate(path: String, size: Long): Int = fs(s"truncate $path .. $size") {
    backend.entry(path) match
      case None => ENOENT
      case Some(_: DirEntry) => EISDIR
      case Some(file: FileEntry) => if (backend.truncate(file.id, size)) OK else EIO // false if called without create or open
  }

  override def write(path: String, source: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = fs(s"write $path .. offset = $offset, size = $size") {
    val intSize = size.toInt.abs // We need to return an Int size, so here it is.
    def data: Iterator[(Long, Array[Byte])] = Iterator.range(0, intSize, memChunk).map { readOffset =>
      val chunkSize = math.min(memChunk, intSize - readOffset)
      offset + readOffset -> new Array[Byte](chunkSize).tap(source.get(readOffset, _, 0, chunkSize))
    }
    if offset < 0 || size != intSize then EOVERFLOW // With intSize being .abs (see above) checks for negative size, too.
    else if backend.write(fi.fh.get(), data) then intSize
    else EIO // false if called without create or open.
  }

  override def unlink(path: String): Int = fs(s"unlink $path") {
    backend.entry(path) match
      case None => ENOENT
      case Some(_: DirEntry) => EISDIR
      case Some(file: FileEntry) =>
        if backend.deleteChildless(file) then OK
        else { log.warn(s"Can't delete regular file with children: $path"); ENOTEMPTY }
  }
