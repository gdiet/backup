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
import ru.serce.jnrfuse.{FuseFillDir, FuseStubFS}

import scala.util.Using.{resource, resources}

object Server extends App {
  private val log = LoggerFactory.getLogger("dedup.Dedup")

  val (options, commands) = args.partition(_.contains("=")).pipe { case (options, commands) =>
    options.map(_.split("=", 2).pipe(o => o(0).toLowerCase -> o(1))).toMap ->
    commands.toList.map(_.toLowerCase())
  }

  if (commands == List("check")) {
    Utils.memoryCheck()

  } else if (commands == List("init")) {
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    require(repo.isDirectory, s"$repo must be a directory.")
    val dbDir = Database.dbDir(repo)
    if (dbDir.exists()) throw new IllegalStateException(s"Database directory $dbDir already exists.")
    resource(H2.file(dbDir, readonly = false)) (Database.initialize)
    log.info(s"Database initialized in $dbDir.")

  } else if (commands == List("dbbackup")) {
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    require(repo.isDirectory, s"$repo must be a directory.")
    DBMaintenance.createBackup(repo)
    log.info(s"Database backup finished.")

  } else if (commands == List("dbrestore")) {
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    require(repo.isDirectory, s"$repo must be a directory.")
    DBMaintenance.restoreBackup(repo, options.get("from"))
    log.info(s"Database restore finished.")

  } else if (commands == List("reclaimspace1")) {
    val keepDeletedDays = options.getOrElse("keepdays", "0").toInt
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    val dbDir = Database.dbDir(repo)
    resource(H2.file(dbDir, readonly = false)) (DBMaintenance.reclaimSpace1(_, keepDeletedDays))

  } else if (commands == List("reclaimspace2")) {
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    val dbDir = Database.dbDir(repo)
    resources(H2.file(dbDir, readonly = false), new LongTermStore(LongTermStore.ltsDir(repo), false)) {
      case (db, lts) => DBMaintenance.reclaimSpace2(db, lts)
    }

  } else if (commands == List("blacklist")) {
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    val blacklistFolder = options.getOrElse("blacklistfolder", "blacklist")
    val dbDir = Database.dbDir(repo)
    resource(H2.file(dbDir, readonly = false)) (DBMaintenance.blacklist(_, blacklistFolder))

  } else if (commands == List("stats")) {
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    val dbDir = Database.dbDir(repo)
    resource(H2.file(dbDir, readonly = false)) (Database.stats)

  } else {
    require(commands.isEmpty || commands == List("write"), s"Unexpected command(s): $commands")
    Utils.asyncLogFreeMemory()
    copyWhenMoving.set(options.get("copywhenmoving").contains("true"))
    val readonly = !commands.contains("write")
    val mountPoint = options.getOrElse("mount", if (getNativePlatform.getOS == OS.WINDOWS) "J:\\" else "/tmp/mnt")
    val absoluteRepo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    val temp = new File(options.getOrElse("temp", sys.props("java.io.tmpdir") + "/dedupfs-temp"))
    if (!readonly) {
      temp.mkdirs()
      require(temp.isDirectory && temp.canWrite, s"Temp dir is not a writable directory: $temp")
      if (temp.list.nonEmpty) log.info(s"Note that temp dir is not empty: $temp")
    }
    if (Platform.getNativePlatform.getOS != WINDOWS) {
      val mountDir = new File(mountPoint)
      require(mountDir.isDirectory, s"Mount point is not a directory: $mountPoint")
      require(mountDir.list.isEmpty, s"Mount point is not empty: $mountPoint")
    }
    log.info (s"Starting dedup file system.")
    log.info (s"Repository:  $absoluteRepo")
    log.info (s"Mount point: $mountPoint")
    log.info (s"Readonly:    $readonly")
    log.debug(s"Temp dir:    $temp")
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

  private val db = new Database(H2.file(dbDir, readonly))
  private val hashAlgorithm = "MD5"
  private val rights = if (readonly) 365 else 511 // o555 else o777
  private val dataDir = LongTermStore.ltsDir(repo)
  private val lts = new LongTermStore(dataDir, readonly)

  private def split(path: String): Array[String] = path.split("/").filter(_.nonEmpty)
  private def entry(path: String): Option[TreeEntry] = entry(split(path))
  private def entry(path: Array[String]): Option[TreeEntry] =
    path.foldLeft[Option[TreeEntry]](Some(Database.root)) {
      case (Some(dir: DirEntry), name) => db.child(dir.id, name)
      case _ => None
    }

  private def guard(msg: String)(f: => Int): Int =
    try f.tap(result => log.trace(s"$msg -> $result"))
    catch { case e: Throwable => log.error(s"$msg -> ERROR", e); EIO }

  override def umount(): Unit =
    guard(s"Unmount repository from $mountPoint") {
      super.umount()
      lts.close()
      log.info(s"Dedup file system is stopped.")
      OK
    }

  /* Note: Calling FileStat.toString DOES NOT WORK, there's a PR: https://github.com/jnr/jnr-ffi/pull/176 */
  override def getattr(path: String, stat: FileStat): Int =
    guard(s"getattr $path") {
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

  override def utimens(path: String, timespec: Array[Timespec]): Int = if (readonly) EROFS else
    guard(s"utimens $path") { // see man UTIMENSAT(2)
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
    guard(s"readdir $path $offset") {
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

  override def rmdir(path: String): Int = if (readonly) EROFS else guard("rmdir $path") {
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
    guard(s"rename $oldpath .. $newpath") {
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

  override def mkdir(path: String, mode: Long): Int = if (readonly) EROFS else guard(s"mkdir $path") {
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
  private var startOfFreeData = db.startOfFreeData
  log.info(s"Data stored: ${readableBytes(startOfFreeData)}")
  log.info(s"Dedup file system is started.")

  // 2020.09.18 perfect
  override def create(path: String, mode: Long, fi: FuseFileInfo): Int =
    guard(s"create $path") {
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
            case None =>
              val id = db.mkFile(dir.id, name, now)
              log.trace(s"create() - create handle for $id $path")
              fileHandles.create(id, Parts(Seq())); OK
          }
      }
    }

  // 2020.09.18 perfect
  override def open(path: String, fi: FuseFileInfo): Int =
    guard(s"open $path") {
      entry(path) match {
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) =>
          log.trace(s"open() - create handle for ${file.id} $path")
          fileHandles.createOrIncCount(file.id, db.parts(file.dataId)); OK
      }
    }

  // 2020.09.18 good
  override def release(path: String, fi: FuseFileInfo): Int =
    guard(s"release $path") {
      entry(path) match {
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) =>
          log.trace(s"release() - decCount for ${file.id} $path")
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
              db.dataEntry(hash, entry.size) match {
                // 4. already known, simply link
                case Some(dataId) =>
                  log.trace(s"release: $path - content known, linking to dataId $dataId")
                  if (file.dataId != dataId) db.setDataId(file.id, dataId)
                // 5. not yet known, store
                case None =>
                  // 5a. reserve storage space
                  val start = startOfFreeData.tap(_ => startOfFreeData += entry.size)
                  // 5b. write to storage
                  entry.read(0, entry.size, lts.read).foldLeft(0L) { case (position, data) =>
                    lts.write(start + position, data)
                    position + data.length
                  }
                  // 5c. create data entry
                  val dataId = db.newDataIdFor(file.id)
                  db.insertDataEntry(dataId, 1, entry.size, start, start + entry.size, hash)
                  log.trace(s"release: $path - new content, dataId $dataId")
              }
            }
            // END asynchronously executed release block
          })
          OK
      }
    }

  // 2020.09.18 perfect
  override def write(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    guard(s"write $path .. offset = $offset, size = $size") {
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
    guard(s"read $path .. offset = $offset, size = $size") {
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
    guard(s"truncate $path .. $size") {
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
    guard(s"unlink $path") {
      if (readonly) EROFS
      else entry(path) match {
        case None => ENOENT
        case Some(_: DirEntry) => EISDIR
        case Some(file: FileEntry) =>
          if (!db.delete(file.id)) EIO
          else {
            log.trace(s"unlink() - drop handle for ${file.id} $path")
            fileHandles.delete(file.id)
            OK
          }
      }
    }
}
