package dedup

import dedup.db.H2
import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.Platform.getNativePlatform

import java.io.File
import scala.concurrent.Future

@main def init(opts: (String, String)*): Unit = guard {
  if opts.isEmpty then main.info("Initializing dedup file system database.")
  else main.info("Initializing dedup file system database, options: " + opts.forOutput)
  val repo  = opts.repo
  val dbDir = db.dbDir(repo)
  if dbDir.exists() then main.failureExit(s"Database directory $dbDir exists - repository is probably already initialized.")
  store.dataDir(repo).mkdirs() // If dataDir is missing, Server.statfs will report free size 0 on Windows.
  scala.util.Using.resource(db.H2.connection(dbDir, readOnly = false, expectExists = false))(db.initialize)
  main.info(s"Database initialized for repository $repo.")
}

@main def stats(opts: (String, String)*): Unit = guard {
  db.maintenance.stats(opts.dbDir)
}

@main def fsc(opts: String*): Unit = guard {
  val dbDir = opts.baseOptions.dbDir
  val cmd = opts.additionalOptions.toList
  cmd match
    case "backup"      ::             params => BackupTool.backup(opts.baseOptions, params)
    case "db-backup"   ::             Nil    => db.maintenance.dbBackup(dbDir).await
    case "db-restore"  ::             Nil    => db.maintenance.restorePlainDbBackup(dbDir)
    case "db-restore"  :: fileName :: Nil    => db.maintenance.restoreSqlDbBackup(dbDir, fileName)
    case "db-compact"  ::             Nil    => db.maintenance.compactDb(dbDir)
    case "find"        :: matcher  :: Nil    => db.maintenance.find(dbDir, matcher)
    case "list"        :: path     :: Nil    => db.maintenance.list(dbDir, path)
    case "del"         :: path     :: Nil    => db.maintenance.del(dbDir, path)
    case _ => println(s"Command '${cmd.mkString(" ")}' not available - missing parameters? Exiting...")
}

@main def dbMigrateStep1(opts: (String, String)*): Unit = guard {
  db.maintenance.migrateDbStep1(opts.dbDir)
}

@main def dbMigrateStep2(opts: (String, String)*): Unit = guard {
  db.maintenance.migrateDbStep2(opts.dbDir)
}

@main def reclaimSpace(opts: (String, String)*): Unit = guard {
  // All required logging is done in the called functions.
  val dbDir = opts.dbDir.tap(main.checkDbDir(_, false))
  db.maintenance.dbBackup(dbDir, H2.dbRef, H2.dbRef.beforeReclaim).await
  db.maintenance.reclaimSpace(dbDir, opts.unnamedOrGet("keepDays").getOrElse("0").toInt)
}

@main def blacklist(opts: (String, String)*): Unit = guard {
  // All required logging is done in the called functions.
  // Here and in other places: '.getCanonicalPath' fails fast for illegal file names like ["/hello].
  val dbDir        = opts.dbDir.tap(main.checkDbDir(_, false))
  val blacklistDir = File(opts.repo, opts.getOrElse("blacklistDir", "blacklist")).getCanonicalPath
  val deleteFiles  = opts.defaultTrue("deleteFiles")
  val dfsBlacklist = opts.getOrElse("dfsBlacklist", "blacklist")
  val deleteCopies = opts.defaultFalse("deleteCopies")
  if opts.defaultTrue("dbBackup") then db.maintenance.dbBackup(dbDir, H2.dbRef, H2.dbRef.beforeBlacklisting).await
  db.blacklist(dbDir, blacklistDir, deleteFiles, dfsBlacklist, deleteCopies)
}

@main def mount(opts: (String, String)*): Unit =
  val readOnly       = opts.defaultFalse("readOnly")
  val copyWhenMoving = java.util.concurrent.atomic.AtomicBoolean(opts.defaultFalse("copyWhenMoving"))
  if opts.defaultFalse("gui") then ServerGui(copyWhenMoving, readOnly)
  
  try
    val isWindows = getNativePlatform.getOS == WINDOWS
    val mount     = File(opts.unnamedOrGet("mount").getOrElse(if isWindows then "J:\\" else "/mnt/dedupfs" )).getCanonicalFile
    if isWindows then
      if !mount.toString.matches(raw"[a-zA-Z]:\\.*") then main.failureExit(s"Mount point not on a local drive: $mount")
      if mount.exists then main.failureExit(s"Mount point already exists: $mount")
      if Option(mount.getParentFile).exists(!_.isDirectory) then main.failureExit(s"Mount parent is not a directory: $mount")
    else
      if !mount.isDirectory then main.failureExit(s"Mount point is not a directory: $mount")
      if !mount.list.isEmpty then main.failureExit(s"Mount point is not empty: $mount")
    if !readOnly then cache.MemCache.startupCheck()
    
    val repo     = opts.repo
    val backup   = !readOnly && opts.defaultTrue("dbBackup")
    val temp     = main.prepareTempDir(readOnly, opts)
    val dbDir -> backupFuture = main.prepareDbDir(repo, backup = backup, readOnly = readOnly)
    val settings = server.Settings(repo, dbDir, temp, readOnly, copyWhenMoving)
    
    main.info (s"Dedup file system settings:")
    main.info (s"Repository:  $repo")
    main.info (s"Mount point: $mount")
    main.info (s"Readonly:    $readOnly")
    main.debug(s"Temp dir:    $temp")
    
    if copyWhenMoving.get() then main.info(s"Copy instead of move initially enabled.")
    val fs             = server.Server(settings)
    val nativeFuseOpts = if getNativePlatform.getOS == WINDOWS then Array("-o", "volname=DedupFS") else Array[String]()
    val fuseOpts       = nativeFuseOpts ++ Array("-o", "big_writes", "-o", "max_write=131072")
    main.info(s"Starting the dedup file system now...")
    
    try fs.mount(mount.toPath, true, false, fuseOpts)
    catch { case t: Throwable => fs.umount(); throw t }
    finally
      if !backupFuture.isCompleted then main.info("Waiting for database backup to finish...")
      backupFuture.await

  catch
    case _: EnsureFailed | main.FailureExit => // already logged
      main.error("Finished abnormally.")
      finishLogging()
    case t: Throwable =>
      main.error("Mount exception, finished abnormally:", t)
      finishLogging()

object main extends util.ClassLogging:
  export log.{debug, info, warn, error}
  object FailureExit extends RuntimeException()
  def failureExit(msg: String*): Nothing = { msg.foreach(log.error(_)); throw FailureExit }

  def prepareTempDir(readOnly: Boolean, opts: Seq[(String, String)]): File =
    File(opts.getOrElse("temp", sys.props("java.io.tmpdir") + s"/dedupfs-temp/$now")).tap { temp =>
      if !readOnly then
        temp.mkdirs()
        if !temp.isDirectory then failureExit(s"Temp dir is not a directory: $temp")
        if !temp.canWrite then failureExit(s"Temp dir is not writable: $temp")
        if temp.list.nonEmpty then warn(s"Note that temp dir is not empty: $temp")
    }

  def prepareDbDir(repo: File, backup: Boolean, readOnly: Boolean): (File, Future[Unit]) =
    db.dbDir(repo).tap(checkDbDir(_, readOnly)).pipe { dbDir =>
      dbDir -> (if backup then db.maintenance.dbBackup(dbDir) else Future.successful(()))
    }

  // TODO check whether we should use this in more places
  // TODO and/or make it part of the command line args .dbDir handling
  def checkDbDir(dbDir: File, readOnly: Boolean): Unit =
    if !dbDir.exists() then main.failureExit(s"It seems the repository is not initialized - can't find the database directory: $dbDir")
    if !readOnly then db.H2.checkForTraceFile(dbDir)

private def guard(f: => Any): Unit =
  try { f; finishLogging() }
  catch
    case throwable =>
      throwable match
        case _: EnsureFailed | main.FailureExit => // Already logged
        case other => main.error("Uncaught exception:", other)
      main.error("Finished abnormally.")
      finishLogging()
      System.exit(1)

/** Base options are 'key=value' unless the equals sign is escaped with a backslash. */
// (.+?)(?<!\\)=(.*)  explained:
// (.+?)              any string of at least one character as group 1, reluctant match
//      (?<!\\)=      negative lookbehind, don't accept an equals sign '=' if preceded by a backslash
//              (.*)  any string of at least one character as group 2
private val baseOptionMatcher = """(.+?)(?<!\\)=(.*)""".r

private def unescapeEquals(string: String): String = string.replaceAll("""\\=""", "=")

given scala.util.CommandLineParser.FromString[(String, String)] with
  def fromString(option: String): (String, String) = option match
    case baseOptionMatcher(key, value) => key.toLowerCase -> unescapeEquals(value)
    case value => "" -> unescapeEquals(value)

extension(options: Seq[String])
  private def baseAndAdditionalOptions = options.partitionMap {
    case baseOptionMatcher(key, value) => Left(key.toLowerCase -> unescapeEquals(value))
    case other => Right(unescapeEquals(other))
  }
  /** Key/value options separated by the equals sign '=' unless the equals sign is escaped with a backslash. */
  private def baseOptions: Seq[(String, String)] =
    baseAndAdditionalOptions._1
  /** The remaining options that are not base options. */
  private def additionalOptions: Seq[String] =
    baseAndAdditionalOptions._2

extension(options: Seq[(String, String)])
  private def opts =
    val map = options.toMap.map((key, value) => key.toLowerCase -> value)
    ensure("unnamed.arguments", options.size == map.size, s"Multiple unnamed arguments: $options")
    map
  private def forOutput: String = opts.map(_+"="+_).mkString(", ")
  private def get(name: String): Option[String] =
    opts.get(name.toLowerCase)
  private def unnamedOrGet(name: String): Option[String] =
    get("").orElse(get(name))
  private def getOrElse(name: String, otherwise: => String): String =
    opts.getOrElse(name.toLowerCase, otherwise)
  private def defaultFalse(name: String): Boolean =
    opts.getOrElse(name.toLowerCase, "false").equalsIgnoreCase("true")
  private def defaultTrue(name: String): Boolean =
    opts.getOrElse(name.toLowerCase, "true").equalsIgnoreCase("true")
  private def repo: File =
    File(opts.getOrElse("repo", "..")).getCanonicalFile
  private def dbDir: File =
    db.dbDir(repo)
