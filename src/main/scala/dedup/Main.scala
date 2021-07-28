package dedup

import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.Platform.getNativePlatform

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Using.resource

@main def init(opts: (String, String)*) =
  val repo  = opts.repo
  val dbDir = db.dbDir(repo)
  if dbDir.exists() then main.failureExit(s"Database directory $dbDir exists - repository is probably already initialized.")
  store.dataDir(repo).mkdirs() // If dataDir is missing, Server.statfs will report free size 0 on Windows.
  resource(db.H2.connection(dbDir, readonly = false))(db.initialize)
  main.info(s"Database initialized for repository $repo.")
  Thread.sleep(200) // Give logging some time to display message

@main def stats(opts: (String, String)*) =
  db.maintenance.stats(opts.dbDir)
  Thread.sleep(200) // Give logging some time to display message

@main def dbRestore(opts: (String, String)*) =
  db.maintenance.restoreBackup(opts.dbDir, opts.get("from"))
  Thread.sleep(200) // Give logging some time to display message

@main def reclaimSpace1(opts: (String, String)*) =
  db.maintenance.backup(opts.dbDir)
  db.maintenance.reclaimSpace1(opts.dbDir, opts.getOrElse("keepDays", "0").toInt)
  Thread.sleep(200) // Give logging some time to display message

@main def reclaimSpace2(opts: (String, String)*) =
  resource(store.LongTermStore(store.dataDir(opts.repo), false))(lts =>
    db.maintenance.reclaimSpace2(opts.dbDir, lts)
  )
  Thread.sleep(200) // Give logging some time to display message

/* TODO
  } else if (commands == List("blacklist")) {
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    val blacklistFolder = options.getOrElse("blacklistfolder", "blacklist")
    val deleteCopies = options.get("deletecopies").contains("true")
    val dbDir = Database.dbDir(repo)
    resource(H2.file(dbDir, readonly = false)) (DBMaintenance.blacklist(_, blacklistFolder, deleteCopies))
    
  def blacklist(connection: Connection, blacklistFolder: String, deleteCopies: Boolean): Unit = resource(connection.createStatement()) { stat =>
    val db = new Database(connection)
    db.child(root.id, blacklistFolder) foreach { blacklistRoot =>
      def recurse(parentPath: String, parentId: Long): Unit = {
        db.children(parentId).foreach {
          case dir: DirEntry =>
            recurse(s"$parentPath/${dir.name}", dir.id)
          case file: FileEntry =>
            val size = stat.executeQuery(s"SELECT stop - start FROM DataEntries WHERE ID = ${file.dataId}")
              .seq(_.getLong(1)).sum
            if (size > 0) {
              log.info(s"Blacklisting $parentPath/${file.name}")
              connection.transaction {
                stat.executeUpdate(s"DELETE FROM DataEntries WHERE id = ${file.dataId} AND seq > 1")
                stat.executeUpdate(s"UPDATE DataEntries SET start = 0, stop = 0 WHERE id = ${file.dataId}")
              }
            }
            if (deleteCopies) {
              @annotation.tailrec
              def pathOf(id: Long, pathEnd: String): String = {
                val parentId -> name = stat.executeQuery(s"SELECT parentId, name FROM TreeEntries WHERE id = $id")
                  .one(r => (r.getLong(1), r.getString(2)))
                val path = s"/$name$pathEnd"
                if (parentId == 0) path else pathOf(parentId, path)
              }
              val copies = stat.executeQuery(s"SELECT id, parentId, name FROM TreeEntries WHERE dataId = ${file.dataId} AND deleted = 0 AND id != ${file.id}")
                .seq(r => (r.getLong(1), r.getLong(2), r.getString(3)))
              if (size == 0 && copies.nonEmpty) log.info(s"Blacklisting $parentPath/${file.name}")
              copies.foreach { case (id, parentId, name) =>
                log.info(s"Deleting copy in ${pathOf(parentId, "/")}$name")
                db.delete(id)
              }
            }
        }
      }
      log.info(s"Start blacklisting /$blacklistFolder")
      recurse(s"/$blacklistFolder", blacklistRoot.id)
      log.info(s"Finished blacklisting /$blacklistFolder")
    }
  }
 */

@main def mount(opts: (String, String)*) =
  def isWindows = getNativePlatform.getOS == WINDOWS
  val repo           = opts.repo
  val mount          = File(opts.getOrElse("mount", if isWindows then "J:\\" else "/mnt/dedupfs" )).getCanonicalFile
  val readOnly       = opts.boolean("readOnly")
  val backup         = !readOnly && !opts.boolean("noDbBackup")
  val copyWhenMoving = opts.boolean("copyWhenMoving")
  val temp           = File(opts.getOrElse("temp", sys.props("java.io.tmpdir") + s"/dedupfs-temp/$now"))
  val gui            = opts.boolean("gui")
  val dbDir          = db.dbDir(repo)
  if !dbDir.exists() then main.failureExit(s"It seems the repository is not initialized - can't find the database directory: $dbDir")
  if isWindows then
    if !mount.toString.matches(raw"[a-zA-Z]:\\.*") then main.failureExit(s"Mount point not on a local drive: $mount")
    if mount.exists then main.failureExit(s"Mount point already exists: $mount")
    if Option(mount.getParentFile).exists(!_.isDirectory) then main.failureExit(s"Mount parent is not a directory: $mount")
  else
    if !mount.isDirectory  then main.failureExit(s"Mount point is not a directory: $mount")
    if !mount.list.isEmpty then main.failureExit(s"Mount point is not empty: $mount")
  val settings = server.Settings(repo, dbDir, temp, readOnly, AtomicBoolean(copyWhenMoving))
  if !readOnly then
    temp.mkdirs()
    if !temp.isDirectory  then main.failureExit(s"Temp dir is not a directory: $temp")
    if !temp.canWrite     then main.failureExit(s"Temp dir is not writable: $temp")
    if temp.list.nonEmpty then main.warn(s"Note that temp dir is not empty: $temp")
  if gui then ServerGui(settings)
  cache.MemCache.startupCheck
  if backup then db.maintenance.backup(settings.dbDir)
  main.info (s"Starting dedup file system.")
  main.info (s"Repository:  $repo")
  main.info (s"Mount point: $mount")
  main.info (s"Readonly:    $readOnly")
  main.debug(s"Temp dir:    $temp")
  if copyWhenMoving then main.info(s"Copy instead of move initially enabled.")
  val fs             = server.Server(settings)
  val nativeFuseOpts = if getNativePlatform.getOS == WINDOWS then Array("-o", "volname=DedupFS") else Array[String]()
  val fuseOpts       = nativeFuseOpts ++ Array("-o", "big_writes", "-o", "max_write=131072")
  try fs.mount(mount.toPath, true, false, fuseOpts)
  catch (e: Throwable) => { main.error("Mount exception:", e); fs.umount() }

object main extends util.ClassLogging:
  export log.{debug, info, warn, error}
  def failureExit(msg: String*): Nothing =
    msg.foreach(log.error(_))
    Thread.sleep(200) // Give logging some time to display message
    sys.exit(1)

given scala.util.CommandLineParser.FromString[(String, String)] with
  private val matcher = """(\w+)=(\S+)""".r
  def fromString(option: String) = option match
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
