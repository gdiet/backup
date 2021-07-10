package dedup

import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.Platform.getNativePlatform

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Using.resource

@main def init(repo: File) =
  val dbDir = db.dbDir(repo)
  if dbDir.exists() then
    main.failureExit(s"Database directory $dbDir exists - repository is probably already initialized.");
  resource(db.H2.connection(dbDir, readonly = false))(db.initialize)
  main.info(s"Database initialized for repository $repo.")

@main def mount(repo: File, mountPoint: File, options: (String, String)*) =
  val opts           = options.toMap
  val readOnly       = opts.get("readonly").contains("true")
  val backup         = !readOnly && !opts.get("nodbbackup").contains("true")
  val copyWhenMoving = opts.get("copywhenmoving").contains("true")
  val temp           = File(opts.getOrElse("temp", sys.props("java.io.tmpdir") + s"/dedupfs-temp/$now"))
  val dbDir          = db.dbDir(repo)
  if !dbDir.exists() then
    main.failureExit(s"It seems the repository is not initialized - can't find the database directory: $dbDir")
  if getNativePlatform.getOS != WINDOWS then
    if !mountPoint.isDirectory  then main.failureExit(s"Mount point is not a directory: $mountPoint")
    if !mountPoint.list.isEmpty then main.failureExit(s"Mount point is not empty: $mountPoint")
  val settings = server.Settings(repo, dbDir, temp, readOnly, AtomicBoolean(copyWhenMoving))
  if !readOnly then
    temp.mkdirs()
    if !temp.isDirectory  then main.failureExit(s"Temp dir is not a directory: $temp")
    if !temp.canWrite     then main.failureExit(s"Temp dir is not writable: $temp")
    if temp.list.nonEmpty then main.warn(s"Note that temp dir is not empty: $temp")
  if backup then db.maintenance.backup(repo)
  // TODO add server GUI
  main.info (s"Starting dedup file system.")
  main.info (s"Repository:  $repo")
  main.info (s"Mount point: $mountPoint")
  main.info (s"Readonly:    $readOnly")
  main.debug(s"Temp dir:    $temp")
  if copyWhenMoving then main.info(s"Copy instead of move initially enabled.")
  val fs             = server.Server(settings)
  val nativeFuseOpts = if getNativePlatform.getOS == WINDOWS then Array("-o", "volname=DedupFS") else Array[String]()
  val fuseOpts       = nativeFuseOpts ++ Array("-o", "big_writes", "-o", "max_write=131072")
  try fs.mount(mountPoint.toPath, true, false, fuseOpts)
  catch (e: Throwable) => { main.error("Mount exception:", e); fs.umount() }

object main extends util.ClassLogging {
  export log.{debug, info, warn, error}
  def failureExit(msg: String*): Nothing =
    msg.foreach(log.error(_))
    Thread.sleep(200)
    sys.exit(1)
}

given scala.util.CommandLineParser.FromString[File] with
  def fromString(file: String) = File(file).getAbsoluteFile()

given scala.util.CommandLineParser.FromString[(String, String)] with
  private val matcher = """-(\S+?)=(\S+)""".r
  def fromString(option: String) = option match
    case matcher(key, value) => key.toLowerCase -> value.toLowerCase
    case _ => throw IllegalArgumentException()
