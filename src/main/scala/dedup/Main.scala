package dedup

import dedup.store.LongTermStore
import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.Platform.getNativePlatform

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Using.{resource, resources}

/** Options are arguments of the type 'xx=abc', commands are arguments without '='.
  * Options and commands are handled case insensitively. Internally they are converted
  * to lower case. */
object Main extends App with ClassLogging {
  // FIXME review uses of require and assert

  def failureExit(msg: String*): Nothing = { msg.foreach(log.error(_)); Thread.sleep(200); sys.exit(1) }

  try {
    val (options, commands) = args.partition(_.contains("=")).pipe { case (options, commands) =>
      options.map(_.split("=", 2).pipe(o => o(0).toLowerCase -> o(1))).toMap ->
        commands.toList.map(_.toLowerCase())
    }

    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    val dbDir = Database.dbDir(repo)
    if (commands != List("init") && !dbDir.isDirectory) failureExit(s"Repository probably not initialized - can't find the database directory: $dbDir")

    if (!repo.isDirectory) failureExit(s"Repository $repo must be a directory.", s"Specify the repository using the repo=<target> option.")

    commands match {
      case List("init") =>
        if (dbDir.exists()) failureExit(s"Database directory $dbDir exists - repository is probably already initialized.")
        resource(H2.file(dbDir, readonly = false))(Database.initialize)
        log.info(s"Database initialized for repository $repo.")

      case List("dbbackup") =>
        DBMaintenance.createBackup(repo)
        log.info(s"Database backup finished.")

      case List("dbrestore") =>
        DBMaintenance.restoreBackup(repo, options.get("from"))
        log.info(s"Database restore finished.")

      case List("reclaimspace1") =>
        val keepDeletedDays = options.getOrElse("keepdays", "0").toInt
        resource(H2.file(dbDir, readonly = false))(DBMaintenance.reclaimSpace1(_, keepDeletedDays))

      case List("reclaimspace2") =>
        resources(H2.file(dbDir, readonly = false), new LongTermStore(Settings.dataDir(repo), false)) {
          case (db, lts) => DBMaintenance.reclaimSpace2(db, lts)
        }

      case List("stats") =>
        resource(H2.file(dbDir, readonly = true))(Database.stats)

      case Nil | List("write") =>
        val readonly = !commands.contains("write")
        val copyWhenMoving = options.get("copywhenmoving").contains("true")
        val mountPoint = options.getOrElse("mount", if (getNativePlatform.getOS == WINDOWS) "J:\\" else "/tmp/mnt")
        val temp = new File(options.getOrElse("temp", sys.props("java.io.tmpdir") + s"/dedupfs-temp/$now"))
        if (!dbDir.isDirectory) failureExit(s"It seems the repository is not initialized - can't find the database directory: $dbDir")
        if (getNativePlatform.getOS != WINDOWS) {
          val mountDir = new File(mountPoint).getAbsoluteFile
          if (!mountDir.isDirectory) failureExit(s"Mount point is not a directory: $mountPoint")
          if (!mountDir.list.isEmpty) failureExit(s"Mount point is not empty: $mountPoint")
        }
        val settings = Settings(repo, dbDir, temp, readonly, new AtomicBoolean(copyWhenMoving))
        if (!readonly) {
          temp.mkdirs()
          if (!temp.isDirectory || !temp.canWrite) failureExit(s"Temp dir is not a writable directory: $temp")
          if (temp.list.nonEmpty) log.warn(s"Note that temp dir is not empty: $temp")
        }
        if (options.get("gui").contains("true")) new ServerGui(settings)
        log.info(s"Starting dedup file system.")
        log.info(s"Repository:  $repo")
        log.info(s"Mount point: $mountPoint")
        log.info(s"Readonly:    $readonly")
        log.debug(s"Temp dir:    $temp")
        if (copyWhenMoving) log.info(s"Copy instead of move initially enabled.")
        val fs = new Server(settings)
        val fuseOptions: Array[String] =
          Array("-o", "big_writes", "-o", "max_write=131072") ++
            (if (getNativePlatform.getOS == WINDOWS) Array("-o", "volname=DedupFS") else Array())
        try fs.mount(java.nio.file.Paths.get(mountPoint), true, false, fuseOptions)
        catch { case e: Throwable => log.error("Mount exception:", e); fs.umount() }

      case other =>
        failureExit(s"Unexpected command(s): ${other.mkString(", ")}")
    }
  } catch { case e: IllegalArgumentException => failureExit(e.getMessage) }
}
