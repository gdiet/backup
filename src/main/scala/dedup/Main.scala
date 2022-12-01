package dedup

import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.Platform.getNativePlatform

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Using.resource

@main def init(opts: (String, String)*): Unit =
  val repo  = opts.repo
  val dbDir = db.dbDir(repo)
  if dbDir.exists() then main.failureExit(s"Database directory $dbDir exists - repository is probably already initialized.")
  store.dataDir(repo).mkdirs() // If dataDir is missing, Server.statfs will report free size 0 on Windows.
  resource(db.H2.connection(dbDir, readonly = false, expectExists = false))(db.initialize)
  main.info(s"Database initialized for repository $repo.")
  Thread.sleep(200) // Give logging some time to display message

@main def stats(opts: (String, String)*): Unit =
  db.maintenance.stats(opts.dbDir)
  Thread.sleep(200) // Give logging some time to display message

@main def fsc(opts: String*): Unit =
  val dbDir = opts.baseOptions.dbDir
  val cmd   = opts.additionalOptions.toList
  cmd match
    case "backup"     :: params          => BackupTool.backup(opts.baseOptions, params)
    case "db-backup"  ::             Nil => db.maintenance.backup             (dbDir          )
    case "db-restore" ::             Nil => db.maintenance.restorePlainBackup (dbDir          )
    case "db-restore" :: fileName :: Nil => db.maintenance.restoreScriptBackup(dbDir, fileName)
    case "db-compact" ::             Nil => db.maintenance.compactDb          (dbDir          )
    case "find"       :: matcher  :: Nil => db.maintenance.find               (dbDir, matcher )
    case "list"       :: path     :: Nil => db.maintenance.list               (dbDir, path    )
    case "del"        :: path     :: Nil => db.maintenance.del                (dbDir, path    )
    case _ => println(s"Command '${cmd.mkString(" ")}' not recognized, exiting...")
  Thread.sleep(200) // Give logging some time to display final messages

@main def reclaimSpace(opts: (String, String)*): Unit =
  db.maintenance.backup(opts.dbDir, "_before_reclaim")
  db.maintenance.reclaimSpace(opts.dbDir, opts.unnamedOrGet("keepDays").getOrElse("0").toInt)
  Thread.sleep(200) // Give logging some time to display message

@main def blacklist(opts: (String, String)*): Unit =
  val blacklistDir = File(opts.repo, opts.getOrElse("blacklistDir", "blacklist")).getPath
  val deleteFiles  = opts.defaultTrue("deleteFiles")
  val dfsBlacklist = opts.getOrElse("dfsBlacklist", "blacklist")
  val deleteCopies = opts.defaultFalse("deleteCopies")
  if opts.defaultTrue("dbBackup") then db.maintenance.backup(opts.dbDir)
  db.blacklist(opts.dbDir, blacklistDir, deleteFiles, dfsBlacklist, deleteCopies)

@main def mount(opts: (String, String)*): Unit =
  val readOnly       = opts.defaultFalse("readOnly")
  val copyWhenMoving = AtomicBoolean(opts.defaultFalse("copyWhenMoving"))
  if opts.defaultFalse("gui") then ServerGui(copyWhenMoving, readOnly)
  try
    def isWindows = getNativePlatform.getOS == WINDOWS
    val repo           = opts.repo
    val mount          = File(opts.unnamedOrGet("mount").getOrElse(if isWindows then "J:\\" else "/mnt/dedupfs" )).getCanonicalFile
    val backup         = !readOnly && opts.defaultTrue("dbBackup")
    val temp           = File(opts.getOrElse("temp", sys.props("java.io.tmpdir") + s"/dedupfs-temp/$now"))
    val dbDir          = db.dbDir(repo)
    if !dbDir.exists() then main.failureExit(s"It seems the repository is not initialized - can't find the database directory: $dbDir")
    if !readOnly then db.H2.checkForTraceFile(dbDir)
    if isWindows then
      if !mount.toString.matches(raw"[a-zA-Z]:\\.*") then main.failureExit(s"Mount point not on a local drive: $mount")
      if mount.exists then main.failureExit(s"Mount point already exists: $mount")
      if Option(mount.getParentFile).exists(!_.isDirectory) then main.failureExit(s"Mount parent is not a directory: $mount")
    else
      if !mount.isDirectory  then main.failureExit(s"Mount point is not a directory: $mount")
      if !mount.list.isEmpty then main.failureExit(s"Mount point is not empty: $mount")
    val settings = server.Settings(repo, dbDir, temp, readOnly, copyWhenMoving)
    if !readOnly then
      temp.mkdirs()
      if !temp.isDirectory  then main.failureExit(s"Temp dir is not a directory: $temp")
      if !temp.canWrite     then main.failureExit(s"Temp dir is not writable: $temp")
      if temp.list.nonEmpty then main.warn(s"Note that temp dir is not empty: $temp")
    if !settings.readonly then cache.MemCache.startupCheck()
    if backup then db.maintenance.backup(settings.dbDir)
    main.info (s"Dedup file system settings:")
    main.info (s"Repository:  $repo")
    main.info (s"Mount point: $mount")
    main.info (s"Readonly:    $readOnly")
    main.debug(s"Temp dir:    $temp")
    if copyWhenMoving.get() then main.info(s"Copy instead of move initially enabled.")
    val fs             = server.Server2(settings)
    val nativeFuseOpts = if getNativePlatform.getOS == WINDOWS then Array("-o", "volname=DedupFS") else Array[String]()
    val fuseOpts       = nativeFuseOpts ++ Array("-o", "big_writes", "-o", "max_write=131072")
    main.info(s"Starting the dedup file system now...")
    try fs.mount(mount.toPath, true, false, fuseOpts) catch { case t: Throwable => fs.umount(); throw t }
  catch
    case main.exit =>
      main.error("Finished abnormally.")
      Thread.sleep(200) // Give logging some time to display message
    case t: Throwable =>
      main.error("Mount exception:", t)
      Thread.sleep(200) // Give logging some time to display message

object main extends util.ClassLogging:
  export log.{debug, info, warn, error}
  object exit extends RuntimeException
  def failureExit(msg: String*): Nothing = { msg.foreach(log.error(_)); throw exit }

private val baseOptionMatcher = """(\w+)=(\S+)""".r

given scala.util.CommandLineParser.FromString[(String, String)] with
  def fromString(option: String): (String, String) = option match
    case baseOptionMatcher(key, value) => key.toLowerCase -> value.toLowerCase
    case value => "" -> value.toLowerCase

extension(options: Seq[String])
  private def baseAndAdditionalOptions = options.partitionMap {
    case baseOptionMatcher(key, value) => Left(key.toLowerCase -> value.toLowerCase)
    case other => Right(other)
  }
  private def baseOptions: Seq[(String, String)] =
    baseAndAdditionalOptions._1
  private def additionalOptions: Seq[String] =
    baseAndAdditionalOptions._2

extension(options: Seq[(String, String)])
  private def opts =
    val map = options.toMap.map((key, value) => key.toLowerCase -> value)
    ensure("unnamed.arguments", options.size == map.size, s"Multiple unnamed arguments: $options")
    map
  private def unnamedOrGet(name: String): Option[String] =
    opts.get("").orElse(opts.get(name.toLowerCase))
  private def get(name: String): Option[String] =
    opts.get(name.toLowerCase)
  private def getOrElse(name: String, otherwise: => String): String =
    opts.getOrElse(name.toLowerCase, otherwise)
  private def defaultFalse(name: String): Boolean =
    opts.getOrElse(name.toLowerCase, "").equalsIgnoreCase("true")
  private def defaultTrue(name: String): Boolean =
    opts.getOrElse(name.toLowerCase, "true").equalsIgnoreCase("true")
  private def repo: File =
    File(opts.getOrElse("repo", "..")).getCanonicalFile
  private def dbDir: File =
    db.dbDir(repo)
