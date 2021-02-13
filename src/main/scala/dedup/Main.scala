package dedup

import dedup.store.LongTermStore

import java.io.File
import scala.util.Using.{resource, resources}
import jnr.ffi.Platform.getNativePlatform
import jnr.ffi.Platform.OS.WINDOWS

import java.util.concurrent.atomic.AtomicBoolean

/** Options are arguments of the type 'xx=abc', commands are arguments without '='.
  * Options and commands are handled case insensitively. Internally they are converted
  * to lower case. */
object Main extends App with ClassLogging {
  val (options, commands) = args.partition(_.contains("=")).pipe { case (options, commands) =>
    options.map(_.split("=", 2).pipe(o => o(0).toLowerCase -> o(1))).toMap ->
      commands.toList.map(_.toLowerCase())
  }

  val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
  val dbDir = Database.dbDir(repo)

  if (!repo.isDirectory) {
    error_(s"$repo must be a directory.")

  } else if (commands == List("init")) {
    if (dbDir.exists()) error_(s"Database directory $dbDir already exists.")
    else {
      resource(H2.file(dbDir, readonly = false))(Database.initialize)
      info_(s"Database initialized for repository $repo.")
    }

  } else if (commands == List("dbbackup")) {
    DBMaintenance.createBackup(repo)
    info_(s"Database backup finished.")

  } else if (commands == List("dbrestore")) {
    DBMaintenance.restoreBackup(repo, options.get("from"))
    info_(s"Database restore finished.")

  } else if (commands == List("reclaimspace1")) {
    val keepDeletedDays = options.getOrElse("keepdays", "0").toInt
    resource(H2.file(dbDir, readonly = false)) (DBMaintenance.reclaimSpace1(_, keepDeletedDays))

  } else if (commands == List("reclaimspace2")) {
    resources(H2.file(dbDir, readonly = false), new LongTermStore(LongTermStore.ltsDir(repo), false)) {
      case (db, lts) => DBMaintenance.reclaimSpace2(db, lts)
    }

  } else if (commands == List("stats")) {
    resource(H2.file(dbDir, readonly = true)) (Database.stats)

  } else if (commands.isEmpty || commands == List("write")) {
    val readonly       = !commands.contains("write")
    val copyWhenMoving = options.get("copywhenmoving").contains("true")
    val mountPoint     = options.getOrElse("mount", if (getNativePlatform.getOS == WINDOWS) "J:\\" else "/tmp/mnt")
    val temp           = new File(options.getOrElse("temp", sys.props("java.io.tmpdir") + s"/dedupfs-temp/$now"))
    // FIXME make sure that dbdir exists
    if (!readonly) {
      temp.mkdirs() // FIXME get rid of require, log error instead, also below
      require(temp.isDirectory && temp.canWrite, s"Temp dir is not a writable directory: $temp")
      if (temp.list.nonEmpty) warn_(s"Note that temp dir is not empty: $temp")
    }
    if (getNativePlatform.getOS != WINDOWS) {
      val mountDir = new File(mountPoint)
      require(mountDir.isDirectory, s"Mount point is not a directory: $mountPoint")
      require(mountDir.list.isEmpty, s"Mount point is not empty: $mountPoint")
    }
    val settings = Settings(repo, dbDir, temp, readonly, new AtomicBoolean(copyWhenMoving))
    if (options.get("gui").contains("true")) new ServerGui(settings)
    info_ (s"Starting dedup file system.")
    info_ (s"Repository:  $repo")
    info_ (s"Mount point: $mountPoint")
    info_ (s"Readonly:    $readonly")
    debug_(s"Temp dir:    $temp")
    if (copyWhenMoving) info_ (s"Copy instead of move initially enabled.")
    val fs = new Server(settings)
    val fuseOptions: Array[String] = if (getNativePlatform.getOS == WINDOWS) Array("-o", "volname=DedupFS") else Array()
    try fs.mount(java.nio.file.Paths.get(mountPoint), true, false, fuseOptions)
    catch { case e: Throwable => error_("Mount exception:", e); fs.umount() }

  } else error_(s"Unexpected command(s): $commands")
}
