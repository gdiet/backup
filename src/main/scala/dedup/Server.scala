package dedup

import java.io.File
import java.lang.System.{currentTimeMillis => now}
import java.security.MessageDigest

import dedup.Database._
import dedup.store.LongTermStore
import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.Platform.{OS, getNativePlatform}
import jnr.ffi.{Platform, Pointer}
import org.slf4j.LoggerFactory
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs, Timespec}
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}

import scala.util.Using.resource

object Server extends App {
  private val log = LoggerFactory.getLogger("dedup.Serve")

  val (options, commands) = args.partition(_.contains("=")).pipe { case (options, commands) =>
    options.map(_.split("=", 2).pipe(o => o(0).toLowerCase -> o(1))).toMap ->
    commands.toList.map(_.toLowerCase())
  }

  if (commands.contains("check")) {
    Utils.memoryCheck()

  } else if (commands.contains("init")) {
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    val dbDir = Database.dbDir(repo)
    if (dbDir.exists()) throw new IllegalStateException(s"Database directory $dbDir already exists.")
    resource(H2.file(dbDir, readonly = false)) (Database.initialize)
    log.info(s"Database initialized in $dbDir.")

  } else if (commands.contains("reclaimspace1")) {
    val keepDeletedDays = options.getOrElse("keepdays", "0").toInt
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    val dbDir = Database.dbDir(repo)
    resource(H2.file(dbDir, readonly = false)) (DatabaseUtils.reclaimSpace1(_, keepDeletedDays))

  } else if (commands.contains("reclaimspace2")) {
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    val dbDir = Database.dbDir(repo)
    resource(H2.file(dbDir, readonly = false)) (Database.reclaimSpace2)

  } else if (commands.contains("orphandataentrystats")) {
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    val dbDir = Database.dbDir(repo)
    resource(H2.file(dbDir, readonly = true)) (Database.orphanDataEntryStats)

  } else if (commands.contains("deleteorphandataentries")) {
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    val dbDir = Database.dbDir(repo)
    resource(H2.file(dbDir, readonly = false)) (Database.deleteOrphanDataEntries)

  } else if (commands.contains("compactionstats")) {
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    val dbDir = Database.dbDir(repo)
    resource(H2.file(dbDir, readonly = false)) (Database.compactionStats)

  } else if (commands.contains("stats")) {
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    val dbDir = Database.dbDir(repo)
    resource(H2.file(dbDir, readonly = false)) (Database.stats)

  } else {
    Utils.asyncLogFreeMemory()
    copyWhenMoving.set(commands.contains("copywhenmoving"))
    val readonly = !commands.contains("write")
    val mountPoint = options.getOrElse("mount", if (getNativePlatform.getOS == OS.WINDOWS) "J:\\" else "/tmp/mnt")
    val absoluteRepo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    val temp = new File(options.getOrElse("temp", absoluteRepo.getPath + "/dedupfs-temp"))
    log.info (s"Starting dedup file system.")
    log.info (s"Repository:  $absoluteRepo")
    log.info (s"Mount point: $mountPoint")
    log.info (s"Readonly:    $readonly")
    log.debug(s"Temp root:   $temp")
    if (!readonly) { if (temp.exists) Utils.delete(temp); temp.mkdirs() }
    if (copyWhenMoving.get) log.info (s"Copy instead of move initially enabled.")
    val fs = new Server(absoluteRepo, temp, readonly)
    try fs.mount(java.nio.file.Paths.get(mountPoint), true, false)
    catch { case e: Throwable => log.error("Mount exception:", e); fs.umount() }
  }
}

/** @param repo Must be an absolute file (e.g. for getFreeSpace). */
class Server(repo: File, tempDir: File, readonly: Boolean) extends FuseStubFS with FuseConstants {
  private val log = LoggerFactory.getLogger("dedup.Serve")
  private val dbDir = Database.dbDir(repo)
  require(dbDir.exists(), s"Database directory $dbDir does not exist.")

  private val db = new Database(H2.file(dbDir, readonly = false))
  private val dataDir = new File(repo, "data").tap{d => d.mkdirs(); require(d.isDirectory)}
  private val hashAlgorithm = "MD5"
  private val rights = if (readonly) 365 else 511 // o555 else o777
  private val lts = new LongTermStore(dataDir, readonly)

  private def split(path: String): Array[String] = path.split("/").filter(_.nonEmpty)
  private def entry(path: String): Option[TreeEntry] = entry(split(path))
  private def entry(path: Array[String]): Option[TreeEntry] =
    path.foldLeft[Option[TreeEntry]](Some(Database.root)) {
      case (Some(dir: DirEntry), name) => db.child(dir.id, name)
      case _ => None
    }

  private def sync[T](f: => T): T = synchronized(f)
  private def sync(msg: String)(f: => Int): Int = sync {
    try f.tap(result => log.trace(s"$msg -> $result"))
    catch { case e: Throwable => log.error(s"$msg -> ERROR", e); EIO }
  }

  override def umount(): Unit =
    sync(s"Unmount repository from $mountPoint") {
      if (!readonly) lts.writeProtectCompleteFiles(startOfFreeDataAtStart, startOfFreeData)
      lts.close()
      OK
    }

  /* Note: Calling FileStat.toString DOES NOT WORK, there's a PR: https://github.com/jnr/jnr-ffi/pull/176 */
  override def getattr(path: String, stat: FileStat): Int =
    sync(s"getattr $path") {
      def setCommon(time: Long, nlink: Int): Unit = {
        stat.st_nlink.set(nlink)
        stat.st_mtim.tv_sec.set(time / 1000)
        stat.st_mtim.tv_nsec.set((time % 1000) * 1000000)
        stat.st_uid.set(getContext.uid.get)
        stat.st_gid.set(getContext.gid.get)
      }
      entry(path) match {
        case None => ENOENT
        case Some(dir: DirEntry) =>
          stat.st_mode.set(FileStat.S_IFDIR | rights)
          setCommon(dir.time, 2)
          OK
        case Some(file: FileEntry) =>
          val size = fileHandles.cacheEntry(file.id).map(_.size).getOrElse(db.dataSize(file.dataId))
          stat.st_mode.set(FileStat.S_IFREG | rights)
          setCommon(file.time, 1)
          stat.st_size.set(size)
          OK
      }
    }

  override def utimens(path: String, timespec: Array[Timespec]): Int =
    sync(s"utimens $path") { // see man UTIMENSAT(2)
      if (timespec.length < 2) EIO
      else {
        val sec = timespec(1).tv_sec.get
        val nan = timespec(1).tv_nsec.longValue
        if (sec < 0 || nan < 0 || nan > 1000000000) EINVAL
        else entry(path) match {
          case None => ENOENT
          case Some(entry) =>
            db.setTime(entry.id, sec*1000 + nan/1000000)
            OK
        }
      }
    }

  /* Note: No benefit expected in implementing opendir/releasedir and handing over the file handle to readdir. */
  override def readdir(path: String, buf: Pointer, fill: FuseFillDir, offset: Long, fi: FuseFileInfo): Int =
    sync(s"readdir $path $offset") {
      entry(path) match {
        case None => ENOENT
        case Some(_: FileEntry) => ENOTDIR
        case Some(dir: DirEntry) =>
          if (offset < 0 || offset.toInt != offset) EOVERFLOW
          else {
            def names = Seq(".", "..") ++ db.children(dir.id).map(_.name)
            // exists: side effect until a condition is met
            names.zipWithIndex.drop(offset.toInt).exists { case (name, k) => fill.apply(buf, name, null, k + 1) != 0 }
            OK
          }
      }
    }

  override def rmdir(path: String): Int = if (readonly) EROFS else sync("rmdir $path") {
    entry(path) match {
      case None => ENOENT
      case Some(_: FileEntry) => ENOTDIR
      case Some(dir: DirEntry) =>
        if (db.children(dir.id).nonEmpty) ENOTEMPTY
        else if (db.delete(dir.id)) OK else EIO
    }
  }

  // Renames a file. Other than the general contract of rename, newpath must not exist.
  override def rename(oldpath: String, newpath: String): Int = if (readonly) EROFS else
    sync(s"rename $oldpath .. $newpath") {
      val (oldParts, newParts) = (split(oldpath), split(newpath))
      if (oldParts.length == 0 || newParts.length == 0) ENOENT
      else entry(oldParts) match {
        case None => ENOENT
        case Some(source) => entry(newParts.take(newParts.length - 1)) match {
          case None => ENOENT
          case Some(_: FileEntry) => ENOTDIR
          case Some(dir: DirEntry) =>
            val newName = newParts.last
            db.child(dir.id, newName) match {
              case Some(_) => EEXIST
              case None =>
                def copy(source: TreeEntry, newName: String, destId: Long): Unit =
                  source match {
                    case file: FileEntry => db.mkFile(destId, newName, file.time, file.dataId)
                    case _: DirEntry =>
                      val id = db.mkDir(destId, newName)
                      db.children(source.id).foreach(child => copy(child, child.name, id))
                  }
                if (source.parent != dir.id && copyWhenMoving.get()) copy(source, newName, dir.id)
                else db.moveRename(source.id, dir.id, newName)
                OK
            }
        }
      }
    }

  override def mkdir(path: String, mode: Long): Int = if (readonly) EROFS else sync(s"mkdir $path") {
    val parts = split(path)
    if (parts.length == 0) ENOENT
    else entry(parts.take(parts.length - 1)) match {
      case None => ENOENT
      case Some(_: FileEntry) => ENOTDIR
      case Some(dir: DirEntry) =>
        val name = parts.last
        db.child(dir.id, name) match {
          case Some(_) => EEXIST
          case None => db.mkDir(dir.id, name); OK
        }
    }
  }

  override def statfs(path: String, stbuf: Statvfs): Int = {
    if (Platform.getNativePlatform.getOS == WINDOWS) {
      // statfs needs to be implemented on Windows in order to allow for copying data from
      // other devices because winfsp calculates the volume size based on the statvfs call.
      // see https://github.com/billziss-gh/winfsp/blob/14e6b402fe3360fdebcc78868de8df27622b565f/src/dll/fuse/fuse_intf.c#L654
      if ("/" == path) {
        stbuf.f_blocks.set(dataDir.getTotalSpace / 32768) // total data blocks in file system
        stbuf.f_frsize.set(32768) // fs block size
        stbuf.f_bfree.set(dataDir.getFreeSpace / 32768) // free blocks in fs
      }
    }
    super.statfs(path, stbuf)
  }.tap(r => log.trace(s"statfs $path -> $r"))

  // #########
  // # Files #
  // #########

  private val fileHandles = new FileHandles(tempDir)
  private val startOfFreeDataAtStart = db.startOfFreeData
  private var startOfFreeData = startOfFreeDataAtStart
  log.info(s"Data stored: ${startOfFreeData / 1000000000}GB")

  // 2020.09.18 perfect
  override def create(path: String, mode: Long, fi: FuseFileInfo): Int =
    sync(s"create $path") {
      val parts = split(path)
      if (readonly) EROFS
      else if (parts.length == 0) ENOENT // can't create root
      else entry(parts.dropRight(1)) match { // fetch parent entry
        case None => ENOENT // parent not known
        case Some(_: FileEntry) => ENOTDIR // parent is a file
        case Some(dir: DirEntry) =>
          val name = parts.last
          db.child(dir.id, name) match {
            case Some(_) => EEXIST // file (or directory with the same name) already exists
            case None => fileHandles.create(db.mkFile(dir.id, name, now), Parts(Seq())); OK
          }
      }
    }

  // 2020.09.18 perfect
  override def open(path: String, fi: FuseFileInfo): Int =
    sync(s"open $path") {
      entry(path) match {
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) => fileHandles.create(file.id, db.parts(file.dataId)); OK
      }
    }

  // 2020.09.18 good
  override def release(path: String, fi: FuseFileInfo): Int =
    sync(s"release $path") {
      entry(path) match {
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) =>
          fileHandles.decCount(file.id, { entry =>
            // START asynchronously executed release block
            // 1. zero size handling - can be the size was > 0 before...
            if (entry.size == 0) db.setDataId(file.id, -1)
            else {
              // 2. calculate hash
              val md = MessageDigest.getInstance(hashAlgorithm)
              entry.read(0, entry.size, lts.read).foreach(md.update)
              val hash = md.digest()
              // 3. check if already known
              sync(db.dataEntry(hash, entry.size)) match {
                // 4. already known, simply link
                case Some(dataId) =>
                  log.trace(s"release: $path - content known, linking to dataId $dataId")
                  if (file.dataId != dataId) sync(db.setDataId(file.id, dataId))
                // 5. not yet known, store
                case None =>
                  // 5a. reserve storage space
                  val start = sync(startOfFreeData.tap(_ => startOfFreeData += entry.size))
                  // 5b. write to storage
                  entry.read(0, entry.size, lts.read).foldLeft(0L) { case (position, data) =>
                    lts.write(start + position, data)
                    position + data.length
                  }
                  // 5c. create data entry
                  sync {
                    val dataId = db.newDataId(file.id)
                    db.insertDataEntry(dataId, 1, entry.size, start, start + entry.size, hash)
                    log.trace(s"release: $path - new content, dataId $dataId")
                  }
              }
            }
            // END asynchronously executed release block
          })
          OK
      }
    }

  // 2020.09.18 perfect
  override def write(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    sync(s"write $path .. offset = $offset, size = $size") {
      val intSize = size.toInt.abs
      if (readonly) EROFS
      else if (offset < 0 || size != intSize) EOVERFLOW
      else entry(path) match {
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) => fileHandles.cacheEntry(file.id) match {
          case None => EIO // write is hopefully only called after create or open
          case Some(cacheEntry) =>
            def dataSource(off: Long, size: Int): Array[Byte] =
              new Array[Byte](size).tap(data => buf.get(off, data, 0, size))
            cacheEntry.write(offset, size, dataSource)
            intSize
        }
      }
    }

  // 2020.09.18 perfect
  override def read(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    sync(s"read $path .. offset = $offset, size = $size") {
      val intSize = size.toInt.abs
      if (offset < 0 || size != intSize) EOVERFLOW
      else entry(path) match {
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) => fileHandles.cacheEntry(file.id) match {
          case None => EIO // read is hopefully only called after create or open
          case Some(cacheEntry) =>
            cacheEntry.read(offset, size, lts.read).foldLeft(0) { case (position, data) =>
              buf.put(position, data, 0, data.length)
              position + data.length
            }
        }
      }
    }

  // 2020.09.18 perfect
  override def truncate(path: String, size: Long): Int =
    sync(s"truncate $path .. $size") {
      if (readonly) EROFS
      else entry(path) match {
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) => fileHandles.cacheEntry(file.id) match {
          case None => EIO // truncate is hopefully only called after create or open
          case Some(cacheEntry) => cacheEntry.truncate(size); OK
        }
      }
    }

  // 2020.09.18 perfect
  override def unlink(path: String): Int =
    sync(s"unlink $path") {
      if (readonly) EROFS
      else entry(path) match {
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) =>
          if (!db.delete(file.id)) EIO
          else { fileHandles.delete(file.id); OK }
      }
    }
}
