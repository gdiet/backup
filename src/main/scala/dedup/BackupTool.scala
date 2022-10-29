package dedup

import dedup.util.ClassLogging

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Using.resource

object BackupTool extends ClassLogging:

  def backup(opts: Seq[(String, String)], params: List[String]): Unit =
    val (from, to, reference) = params match
      case from :: to :: reference :: Nil => (File(from), to, Some(reference))
      case from :: to              :: Nil => (File(from), to, None)
      case other => main.failureExit(s"Expected parameters '[from] [to] [optional: reference]', got '${other.mkString(" ")}'")

//    // TODO lots of copy-paste from '@main def mount(opts: (String, String)*): Unit'
//    val repo = opts.repo
//    val backup = opts.defaultFalse("dbBackup")
//    val temp = File(opts.getOrElse("temp", sys.props("java.io.tmpdir") + s"/dedupfs-temp/$now"))
//    val dbDir = db.dbDir(repo)
//    if !dbDir.exists() then main.failureExit(s"It seems the repository is not initialized - can't find the database directory: $dbDir")
//    db.H2.checkForTraceFile(dbDir)
//    val settings = server.Settings(repo, dbDir, temp, false, AtomicBoolean(false))
//    temp.mkdirs()
//    if !temp.isDirectory then main.failureExit(s"Temp dir is not a directory: $temp")
//    if !temp.canWrite then main.failureExit(s"Temp dir is not writable: $temp")
//    if temp.list.nonEmpty then log.warn(s"Note that temp dir is not empty: $temp")
//    cache.MemCache.startupCheck()
//    if backup then db.maintenance.backup(settings.dbDir)
//    // TODO end of copy-paste

    if !from.canRead then main.failureExit(s"The backup source $from can't be read.")

    log.info (s"Running the backup tool")
    log.info (s"Repository:       $repo")
    log.info (s"Backup source:    $from")
    log.info (s"Backup target:    $to")
    log.info (s"Backup reference: $reference")
//    log.debug(s"Temp dir:         $temp")
//
//    resource(server.Level1(settings)) { fs =>
//      val (targetPath, targetName) = fs.split(to).pipe(target =>
//          target.dropRight(1) -> target.lastOption.getOrElse(main.failureExit(s"Invalid target: No file name in '$to'."))
//      )
//      def targetPathForLog = s"The target's parent DedupFS:/${targetPath.mkString("/")}"
//      val targetParent = fs.entry(targetPath) match
//        case Some(dir: DirEntry) => dir
//        case Some(_: FileEntry) => main.failureExit(s"Invalid target: $targetPathForLog points to a file.")
//        case None               => main.failureExit(s"Invalid target: $targetPathForLog does not exist.")
//      val targetId = fs.mkDir(targetParent.id, targetName).getOrElse(
//        main.failureExit(s"Invalid target: DedupFS:$to already exists.")
//      )
//      log.info(s"Created 'DedupFS:$to'.")
//      // TODO continue
//
//      // .backupignore
//      // if empty, ignore the directory
//      // otherwise, interpret in a .gitignore like fashion
//    }

    processRecurse(Seq(from))

  @annotation.tailrec
  private def processRecurse(sources: Seq[File]): Unit =
    sources match
      case Seq() => /* nothing to do */
      case source +: remaining =>
        def ignoreFile = File(source, ".backupignore")
        if !source.isDirectory then
          log.info(s"Store file: $source")
          processRecurse(remaining)
        else if !ignoreFile.isFile then
          log.info(s"Store dir : $source")
          processRecurse(remaining ++ source.listFiles())
        else if ignoreFile.length() == 0 then
          log.info(s"IGNORE    : $source")
          processRecurse(remaining)
        else
          log.info(s"*******   : $source")
          processRecurse(remaining)
