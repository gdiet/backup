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
  resource(db.H2.connection(dbDir, readonly = false, dbMustExist = false))(db.initialize)
  main.info(s"Database initialized for repository $repo.")
  Thread.sleep(200) // Give logging some time to display message

@main def stats(opts: (String, String)*): Unit =
  db.maintenance.stats(opts.dbDir)
  Thread.sleep(200) // Give logging some time to display message

@main def dbRestore(opts: (String, String)*): Unit =
  db.maintenance.restoreBackup(opts.dbDir, opts.get("from"))
  Thread.sleep(200) // Give logging some time to display message

@main def reclaimSpace1(opts: (String, String)*): Unit =
  db.maintenance.backup(opts.dbDir)
  db.maintenance.reclaimSpace1(opts.dbDir, opts.getOrElse("keepDays", "0").toInt)
  Thread.sleep(200) // Give logging some time to display message

@main def reclaimSpace2(opts: (String, String)*): Unit =
  resource(store.LongTermStore(store.dataDir(opts.repo), false))(lts =>
    db.maintenance.reclaimSpace2(opts.dbDir, lts)
  )
  Thread.sleep(200) // Give logging some time to display message

@main def mount(opts: (String, String)*): Unit =
  val readOnly       = opts.boolean("readOnly")
  val copyWhenMoving = AtomicBoolean(opts.boolean("copyWhenMoving"))
  if opts.boolean("gui") then ServerGui(copyWhenMoving, readOnly)
  try
    def isWindows = getNativePlatform.getOS == WINDOWS
    val repo           = opts.repo
    val mount          = File(opts.getOrElse("mount", if isWindows then "J:\\" else "/mnt/dedupfs" )).getCanonicalFile
    val backup         = !readOnly && !opts.boolean("noDbBackup")
    val temp           = File(opts.getOrElse("temp", sys.props("java.io.tmpdir") + s"/dedupfs-temp/$now"))
    val dbDir          = db.dbDir(repo)
    if !dbDir.exists() then main.failureExit(s"It seems the repository is not initialized - can't find the database directory: $dbDir")
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
    cache.MemCache.startupCheck()
    if backup then db.maintenance.backup(settings.dbDir)
    main.info (s"Starting dedup file system.")
    main.info (s"Repository:  $repo")
    main.info (s"Mount point: $mount")
    main.info (s"Readonly:    $readOnly")
    main.debug(s"Temp dir:    $temp")
    if copyWhenMoving.get() then main.info(s"Copy instead of move initially enabled.")
    val fs             = server.Server(settings)
    val nativeFuseOpts = if getNativePlatform.getOS == WINDOWS then Array("-o", "volname=DedupFS") else Array[String]()
    val fuseOpts       = nativeFuseOpts ++ Array("-o", "big_writes", "-o", "max_write=131072")
    try fs.mount(mount.toPath, true, false, fuseOpts) catch (e: Throwable) => { fs.umount(); throw e }
  catch
    case main.exit =>
      main.error("Finished abnormally.")
      Thread.sleep(200) // Give logging some time to display message
    case e: Throwable =>
      main.error("Mount exception:", e)
      Thread.sleep(200) // Give logging some time to display message

object main extends util.ClassLogging:
  export log.{debug, info, warn, error}
  object exit extends RuntimeException
  def failureExit(msg: String*): Nothing = { msg.foreach(log.error(_)); throw exit }

given scala.util.CommandLineParser.FromString[(String, String)] with
  private val matcher = """(\w+)=(\S+)""".r
  def fromString(option: String): (String, String) = option match
    case matcher(key, value) => key.toLowerCase -> value.toLowerCase
    case _ => throw IllegalArgumentException()

extension(options: Seq[(String, String)])
  private def opts = options.toMap.map((key, value) => key.toLowerCase -> value)
  private def get(name: String): Option[String] =
    opts.get(name.toLowerCase)
  private def getOrElse(name: String, otherwise: => String): String =
    opts.getOrElse(name.toLowerCase, otherwise)
  private def boolean(name: String): Boolean =
    opts.getOrElse(name.toLowerCase, "").equalsIgnoreCase("true")
  private def repo: File =
    File(opts.getOrElse("repo", "..")).getCanonicalFile
  private def dbDir: File =
    db.dbDir(repo)
