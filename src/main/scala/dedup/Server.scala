package dedup

import java.io.File
import java.lang.System.{currentTimeMillis => now}
import java.nio.file.{Files, StandardCopyOption}
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.Date

import dedup.Database._
import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.Platform.{OS, getNativePlatform}
import jnr.ffi.{Platform, Pointer}
import org.slf4j.LoggerFactory
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs, Timespec}
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}

import scala.util.Using.resource

/* TODO Statistics utility:
 * Deleted folders: SELECT count(*) FROM TreeEntries WHERE deleted <> 0 AND dataid is NULL;
 * Deleted files: SELECT count(*) FROM TreeEntries WHERE deleted <> 0 AND dataid IS NOT NULL;
 * Orphan folders: SELECT count(*) FROM TreeEntries a LEFT JOIN TreeEntries b ON a.PARENTID = b.ID WHERE a.deleted = 0 and a.DATAID is null AND b.deleted <> 0;
 * Orphan files: SELECT count(*) FROM TreeEntries a LEFT JOIN TreeEntries b ON a.PARENTID = b.ID WHERE a.deleted = 0 and a.DATAID is not null AND b.deleted <> 0;
 * Total folders: SELECT count(*) FROM TreeEntries WHERE dataid is NULL;
 * Total files: SELECT count(*) FROM TreeEntries WHERE dataid IS NOT NULL;
 * Current folders = total folders - deleted folders - orphan folders
 * Current files = total files - deleted files - orphan files
 * Unreferenced data (slow): ??? SELECT * from DATAENTRIES d left JOIN TREEENTRIES t on d.ID = t.DATAID WHERE t.DATAID is null;
 */
object Server extends App {
  private val log = LoggerFactory.getLogger("dedup.ServerApp")

  import Runtime.{getRuntime => rt}
  def freeMemory: Long = rt.maxMemory - rt.totalMemory + rt.freeMemory

  val (options, commands) = args.partition(_.contains("=")).pipe { case (options, commands) =>
    options.map(_.split("=", 2).pipe(o => o(0).toLowerCase -> o(1))).toMap ->
    commands.toList.map(_.toLowerCase())
  }

  if (commands.contains("check")) {
    // https://stackoverflow.com/questions/58506337/java-byte-array-of-1-mb-or-more-takes-up-twice-the-ram
    log.info("Checking memory management for byte arrays now.")

    System.gc(); Thread.sleep(1000)
    val freeBeforeNormalDataCheck = freeMemory
    val normallyHandledData = Vector.fill(100)(new Array[Byte](1000000))
    System.gc(); Thread.sleep(1000)
    val usedByNormalData = freeBeforeNormalDataCheck - freeMemory
    val deviationWithNormalData = (100000000 - usedByNormalData).abs / 1000000
    log.info(s"${normallyHandledData.size} Byte arrays of size 1000000 used $usedByNormalData bytes of RAM.")
    log.info(s"This is a deviation of $deviationWithNormalData% from the expected value.")
    log.info(s"This software assumes that the deviation is near to 0%.")

    val freeBeforeExceptionalDataCheck = freeMemory
    val exceptionallyHandledData = Vector.fill(100)(new Array[Byte](1048576))
    System.gc(); Thread.sleep(1000)
    val usedByExceptionalData = freeBeforeExceptionalDataCheck - freeMemory
    val deviationWithExceptionalData = (104857600 - usedByExceptionalData).abs / 1000000
    log.info(s"${exceptionallyHandledData.size} Byte arrays of size 1048576 used $usedByExceptionalData bytes of RAM.")
    log.info(s"This is a deviation of $deviationWithExceptionalData% from the expected value.")
    log.info(s"This software assumes that the deviation is near to 100%.")

    require(deviationWithNormalData < 15, "High deviation of memory usage for normal data.")
    require(deviationWithExceptionalData > 80, "Low deviation of memory usage for exceptional data.")
    require(deviationWithExceptionalData < 120, "High deviation of memory usage for exceptional data.")

  } else if (commands.contains("init")) {
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    val dbDir = Database.dbDir(repo)
    if (dbDir.exists()) throw new IllegalStateException(s"Database directory $dbDir already exists.")
    resource(H2.file(dbDir, readonly = false)) (Database.initialize)
    log.info(s"Database initialized in $dbDir.")

  } else {
    asyncLogFreeMemory()
    val readonly = !commands.contains("write")
    val mountPoint = options.getOrElse("mount", if (getNativePlatform.getOS == OS.WINDOWS) "J:\\" else "/tmp/mnt")
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    val temp = new File(options.getOrElse("temp", repo.getPath)).getAbsoluteFile
    val fs = new Server(repo, temp, readonly)
    log.info(s"Starting dedup file system.")
    log.info(s"Repository:  $repo")
    log.info(s"Mount point: $mountPoint")
    log.info(s"Readonly:    $readonly")
    log.info(s"Temp dir:    $temp")
    try fs.mount(java.nio.file.Paths.get(mountPoint), true, false)
    catch { case e: Throwable => log.error("Mount exception:", e); fs.umount() }
  }

  private def asyncLogFreeMemory(): Unit = concurrent.ExecutionContext.global.execute { () =>
    var lastFree = 0L
    while(true) {
      Thread.sleep(5000)
      val free = freeMemory
      if ((free-lastFree).abs * 10 > lastFree) {  lastFree = free; log.info(s"Free memory: ${free/1000000} MB") }
    }
  }
}

// TODO shutdown hook?
class Server(maybeRelativeRepo: File, maybeRelativeTemp: File, readonly: Boolean) extends FuseStubFS {
  private val log = LoggerFactory.getLogger(getClass)
  private val repo = maybeRelativeRepo.getAbsoluteFile // absolute needed e.g. for getFreeSpace()
  private val dbDir = Database.dbDir(repo)
  require(dbDir.exists(), s"Database directory $dbDir does not exist.")
  if (!readonly) {
    val dbFile = new File(dbDir, "dedupfs.mv.db")
    require(dbFile.exists(), s"Database file $dbFile does not exist.")
    val timestamp: String = new SimpleDateFormat("yyyy-MM-dd_HH-mm").format(new Date())
    val backup = new File(dbDir, s"dedupfs_$timestamp.mv.db")
    Files.copy(dbFile.toPath, backup.toPath, StandardCopyOption.COPY_ATTRIBUTES)
    backup.setReadOnly()
    log.info(s"Created database backup file $backup")
  }

  private val db = new Database(H2.file(dbDir, readonly = false))
  private val dataDir = new File(repo, "data").tap{d => d.mkdirs(); require(d.isDirectory)}
  private val store = new DataStore(dataDir.getAbsolutePath, maybeRelativeTemp.getAbsolutePath, readonly)
  private val hashAlgorithm = "MD5"
  private val rights = if (readonly) 365 else 511 // o555 else o777

  private def split(path: String): Array[String] = path.split("/").filter(_.nonEmpty)
  private def entry(path: String): Option[TreeEntry] = entry(split(path))
  private def entry(path: Array[String]): Option[TreeEntry] =
    path.foldLeft[Option[TreeEntry]](Some(Database.root)) {
      case (Some(dir: DirEntry), name) => db.child(dir.id, name)
      case _ => None
    }

  private def sync[T](msg: String)(f: => Int): Int = synchronized(
    try f.tap(result => log.debug(s"$msg -> $result"))
    catch { case e: Throwable => log.error(s"$msg -> ERROR", e); -ErrorCodes.EIO() }
  )

  override def umount(): Unit = sync(s"Unmount repository from $mountPoint") {
    if (!readonly) store.writeProtectCompleteFiles(startOfFreeDataAtStart, startOfFreeData)
    store.close(); 0
  }

  /* Note: Calling FileStat.toString DOES NOT WORK, there's a PR: https://github.com/jnr/jnr-ffi/pull/176 */
  override def getattr(path: String, stat: FileStat): Int = sync(s"getattr $path") {
    def setCommon(time: Long, nlink: Int): Unit = {
      stat.st_nlink.set(nlink)
      stat.st_mtim.tv_sec.set(time / 1000)
      stat.st_mtim.tv_nsec.set((time % 1000) * 1000000)
      stat.st_uid.set(getContext.uid.get)
      stat.st_gid.set(getContext.gid.get)
    }
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(dir: DirEntry) =>
        stat.st_mode.set(FileStat.S_IFDIR | rights)
        setCommon(dir.time, 2)
        0
      case Some(file: FileEntry) =>
        val size = store.size(file.id, file.dataId, db.dataSize(file.dataId))
        stat.st_mode.set(FileStat.S_IFREG | rights)
        setCommon(file.time, 1)
        stat.st_size.set(size)
        0
    }
  }

  override def utimens(path: String, timespec: Array[Timespec]): Int = sync(s"utimens $path") { // see man UTIMENSAT(2)
    if (timespec.length < 2) -ErrorCodes.EIO else {
      val sec = timespec(1).tv_sec.get
      val nan = timespec(1).tv_nsec.longValue
      if (sec < 0 || nan < 0 || nan > 1000000000) -ErrorCodes.EINVAL
      else entry(path) match {
        case None => -ErrorCodes.ENOENT
        case Some(entry) =>
          db.setTime(entry.id, timespec(1).pipe(t => t.tv_sec.get * 1000 + t.tv_nsec.longValue / 1000000))
          0
      }
    }
  }

  /* Note: No benefit expected in implementing opendir/releasedir and handing over the file handle to readdir. */
  override def readdir(path: String, buf: Pointer, fill: FuseFillDir, offset: Long, fi: FuseFileInfo): Int =
    sync(s"readdir $path $offset") {
      entry(path) match {
        case None => -ErrorCodes.ENOENT
        case Some(_: FileEntry) => -ErrorCodes.ENOTDIR
        case Some(dir: DirEntry) =>
          if (offset < 0 || offset.toInt != offset) -ErrorCodes.EOVERFLOW
          else {
            def names = Seq(".", "..") ++ db.children(dir.id).map(_.name)
            // exists: side effect until a condition is met
            names.zipWithIndex.drop(offset.toInt).exists { case (name, k) => fill.apply(buf, name, null, k + 1) != 0 }
            0
          }
      }
    }

  override def rmdir(path: String): Int = if (readonly) -ErrorCodes.EROFS else sync("rmdir $path") {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: FileEntry) => -ErrorCodes.ENOTDIR
      case Some(dir: DirEntry) =>
        if (db.children(dir.id).nonEmpty) -ErrorCodes.ENOTEMPTY
        else if (db.delete(dir.id)) 0 else -ErrorCodes.EIO
    }
  }

  // Renames a file. Other than the general contract of rename, newpath must not exist.
  override def rename(oldpath: String, newpath: String): Int = if (readonly) -ErrorCodes.EROFS else
    sync(s"rename $oldpath .. $newpath") {
      val (oldParts, newParts) = (split(oldpath), split(newpath))
      if (oldParts.length == 0 || newParts.length == 0) -ErrorCodes.ENOENT
      else entry(oldParts) match {
        case None => -ErrorCodes.ENOENT
        case Some(source) => entry(newParts.take(newParts.length - 1)) match {
          case None => -ErrorCodes.ENOENT
          case Some(_: FileEntry) => -ErrorCodes.ENOTDIR
          case Some(dir: DirEntry) =>
            val newName = newParts.last
            db.child(dir.id, newName) match {
              case Some(_) => -ErrorCodes.EEXIST
              case None => db.moveRename(source.id, dir.id, newName); 0
            }
        }
      }
    }

  override def mkdir(path: String, mode: Long): Int = if (readonly) -ErrorCodes.EROFS else sync(s"mkdir $path") {
    val parts = split(path)
    if (parts.length == 0) -ErrorCodes.ENOENT
    else entry(parts.take(parts.length - 1)) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: FileEntry) => -ErrorCodes.ENOTDIR
      case Some(dir: DirEntry) =>
        val name = parts.last
        db.child(dir.id, name) match {
          case Some(_) => -ErrorCodes.EEXIST
          case None => db.mkDir(dir.id, name); 0
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
  }.tap(r => log.debug(s"statfs $path -> $r"))

  // #########
  // # Files #
  // #########

  private var fileDescriptors: Map[Long, Int] = Map()
  private def incCount(id: Long): Unit = fileDescriptors += id -> (fileDescriptors.getOrElse(id, 0) + 1)
  private val startOfFreeDataAtStart = db.startOfFreeData
  private var startOfFreeData = startOfFreeDataAtStart
  log.info(s"Data stored: ${startOfFreeData / 1000000000}GB")

  override def create(path: String, mode: Long, fi: FuseFileInfo): Int = if (readonly) -ErrorCodes.EROFS else
    sync(s"create $path") {
      val parts = split(path)
      if (parts.length == 0) -ErrorCodes.ENOENT
      else entry(parts.take(parts.length - 1)) match {
        case None => -ErrorCodes.ENOENT
        case Some(_: FileEntry) => -ErrorCodes.ENOTDIR
        case Some(dir: DirEntry) =>
          val name = parts.last
          db.child(dir.id, name) match {
            case Some(_) => -ErrorCodes.EEXIST
            case None =>
              val id = db.mkFile(dir.id, name, now)
              incCount(id); 0
          }
      }
    }

  override def open(path: String, fi: FuseFileInfo): Int = sync(s"open $path") {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: DirEntry) => -ErrorCodes.EISDIR
      case Some(file: FileEntry) =>
        incCount(file.id); 0
    }
  }

  override def release(path: String, fi: FuseFileInfo): Int = sync(s"release $path") {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: DirEntry) => -ErrorCodes.EISDIR
      case Some(file: FileEntry) =>
        fileDescriptors.get(file.id).foreach {
          case 1 =>
            fileDescriptors -= file.id
            store.ifDataWritten(file.id, file.dataId) {
              val startStop = db.startStop(file.dataId)
              val size = store.size(file.id, file.dataId, startStop.size)
              val md = MessageDigest.getInstance(hashAlgorithm)
              for { position <- 0L until size by 524288; chunkSize = math.min(524288, size - position).toInt }
                md.update(store.read(file.id, file.dataId, startStop)(position, chunkSize))
              val hash = md.digest()
              db.dataEntry(hash, size) match {
                case Some(dataId) =>
                  log.debug(s"Already known, linking: $path")
                  if (file.dataId != dataId) require(db.setDataId(file.id, dataId))
                case None =>
                  val dataId = if (startStop.isEmpty) file.dataId else db.newDataId(file.id)
                  log.debug(s"release: $path $file -> DATAID $dataId")
                  store.persist(file.id, file.dataId, startStop)(startOfFreeData, size)
                  db.insertDataEntry(dataId, startOfFreeData, startOfFreeData + size, hash)
                  startOfFreeData += size
              }
              store.delete(file.id, file.dataId)
            }
          case n => fileDescriptors += file.id -> (n - 1)
        }
        0
    }
  }

  override def write(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    if (readonly) -ErrorCodes.EROFS else sync(s"write $path .. $offset/$size") {
      entry(path) match {
        case None => -ErrorCodes.ENOENT
        case Some(_: DirEntry) => -ErrorCodes.EISDIR
        case Some(file: FileEntry) =>
          val intSize = size.toInt.abs
          if (!fileDescriptors.contains(file.id)) -ErrorCodes.EIO
          else if (offset < 0 || size != intSize) -ErrorCodes.EOVERFLOW
          else {
            val data = new Array[Byte](intSize)
            buf.get(0, data, 0, intSize)
            store.write(file.id, file.dataId, db.dataSize(file.dataId))(offset, data)
            intSize
          }
      }
    }

  override def read(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = sync(s"read $path .. $size/$offset") {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: DirEntry) => -ErrorCodes.EISDIR
      case Some(file: FileEntry) =>
        if (!fileDescriptors.contains(file.id)) -ErrorCodes.EIO
        else {
          if (offset < 0 || size < 0 || size > 67108864) -ErrorCodes.EOVERFLOW // 64MiB
          else {
            if (size > 16777216) log.warn(s"Conspiciously large read request: $size bytes.") // 16MiB
            val startStop = db.startStop(file.dataId)
            val bytes: Array[Byte] = store.read(file.id, file.dataId, startStop)(offset, size.toInt)
            buf.put(0, bytes, 0, bytes.length)
            bytes.length
          }
        }
    }
  }

  override def truncate(path: String, size: Long): Int = if (readonly) -ErrorCodes.EROFS else sync(s"truncate $path .. $size") {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: DirEntry) => -ErrorCodes.EISDIR
      case Some(file: FileEntry) =>
        store.truncate(file.id, file.dataId, db.startStop(file.dataId), size)
        0
    }
  }

  override def unlink(path: String): Int = if (readonly) -ErrorCodes.EROFS else sync(s"unlink $path") {
    entry(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: DirEntry) => -ErrorCodes.EISDIR
      case Some(file: FileEntry) =>
        if (db.delete(file.id)) {
          store.delete(file.id, file.dataId)
          fileDescriptors -= file.id
          0
        } else -ErrorCodes.EIO
    }
  }
}
