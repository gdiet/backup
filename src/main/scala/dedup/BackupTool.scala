package dedup

import dedup.util.ClassLogging

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import scala.io.Source
import scala.util.Using.resource

object BackupTool extends ClassLogging:

  /** Replace '<...>' by formatting the contents with the SimpleDateFormat of 'now'. */
  private def insertDate(string: String): String =
    val regex = "<.+?>".r
    val date = java.util.Date.from(java.time.Instant.now())
    regex.replaceAllIn(string, { m => java.text.SimpleDateFormat(m.group(0).drop(1).dropRight(1)).format(date) })

  def backup(opts: Seq[(String, String)], params: List[String]): Unit =
    val (from, to, reference) = params match
      case from :: to :: reference :: Nil => (File(from), insertDate(to), Some(reference))
      case from :: to              :: Nil => (File(from), insertDate(to), None)
      case other => main.failureExit(s"Expected parameters '[from] [to] [optional: reference]', got '${other.mkString(" ")}'")

    // TODO lots of copy-paste from '@main def mount(opts: (String, String)*): Unit'
    val repo = opts.repo
    val backup = opts.defaultFalse("dbBackup")
    val temp = File(opts.getOrElse("temp", sys.props("java.io.tmpdir") + s"/dedupfs-temp/$now"))
    val dbDir = db.dbDir(repo)
    if !dbDir.exists() then main.failureExit(s"It seems the repository is not initialized - can't find the database directory: $dbDir")
    db.H2.checkForTraceFile(dbDir)
    val settings = server.Settings(repo, dbDir, temp, false, AtomicBoolean(false))
    temp.mkdirs()
    if !temp.isDirectory then main.failureExit(s"Temp dir is not a directory: $temp")
    if !temp.canWrite then main.failureExit(s"Temp dir is not writable: $temp")
    if temp.list.nonEmpty then log.warn(s"Note that temp dir is not empty: $temp")
    cache.MemCache.startupCheck()
    if backup then db.maintenance.backup(settings.dbDir)
    // TODO end of copy-paste

    if !from.canRead then main.failureExit(s"The backup source $from can't be read.")

    log.info (s"Running the backup tool")
    log.info (s"Repository:       $repo")
    log.info (s"Backup source:    $from")
    log.info (s"Backup target:    $to")
    log.info (s"Backup reference: $reference")
    log.debug(s"Temp dir:         $temp")

    resource(server.Level1(settings)) { fs =>
      val (targetPath, targetName) = fs.split(to).pipe(target =>
          target.dropRight(1) -> target.lastOption.getOrElse(main.failureExit(s"Invalid target: No file name in '$to'."))
      )
      def targetPathForLog = s"The target's parent DedupFS:/${targetPath.mkString("/")}"
      val targetParent = fs.entry(targetPath) match
        case Some(dir: DirEntry) => dir
        case Some(_: FileEntry) => main.failureExit(s"Invalid target: $targetPathForLog points to a file.")
        case None               => main.failureExit(s"Invalid target: $targetPathForLog does not exist.")
      val targetId = fs.mkDir(targetParent.id, targetName).getOrElse(
        main.failureExit(s"Invalid target: DedupFS:$to already exists.")
      )
      log.info(s"Created 'DedupFS:$to'.")

      def mkDir(parent: Long, name: String): Long =
        fs.mkDir(parent, name).getOrElse(main.failureExit(s"Unexpected name conflict creating folder $name."))

      def store(parent: Long, file: File): Unit =
        // TODO implement
        log.info(s"Stored $file")

      processRecurse(Seq((0, Seq(), "/", from)), mkDir, store)
    }


  @annotation.tailrec
  private def processRecurse(sources: Seq[(Long, Seq[String], String, File)], mkDir: (Long, String) => Long, store: (Long, File) => Unit): Unit =
    sources match
      case Seq() => /* nothing to do */
      case (parent, ignore, path, source) +: remaining =>
        def ignoreFile = File(source, ".backupignore")
        def sourcePath = s"$path${source.getName}" + (if source.isDirectory then "/" else "")
        if ignore.exists(sourcePath.matches) then
          log.info(s"Skipping (rule): $sourcePath")
          processRecurse(remaining, mkDir, store)
        else if !source.isDirectory then
          store(parent, source)
          processRecurse(remaining, mkDir, store)
        else if !ignoreFile.isFile then
          val dir = mkDir(parent, source.getName)
          val add = source.listFiles().map((dir, ignore, sourcePath, _))
          processRecurse(remaining ++ add, mkDir, store)
        else if ignoreFile.length() == 0 then
          log.info(s"Skipping (file): $sourcePath")
          processRecurse(remaining, mkDir, store)
        else
          val ignoreRules =
            resource(Source.fromFile(ignoreFile))(_.getLines().toSeq).filter(_.nonEmpty)
          log.info(s"Ignore rules in source path: $sourcePath")
          ignoreRules.zipWithIndex.foreach((rule, index) => log.info(s"Rule ${index+1}: $rule"))
          val additionalIgnore =
            resource(Source.fromFile(ignoreFile))(_.getLines().toSeq)
              .filter(_.nonEmpty)
              .map(_.replaceAll("\\?", "\\\\E.\\\\Q").replaceAll("\\*", "\\\\E.*\\\\Q"))
              .map("\\Q" + _ + "\\E")
          val dir = mkDir(parent, source.getName)
          val add = source.listFiles().map((dir, ignore ++ additionalIgnore, sourcePath, _))
          processRecurse(remaining ++ add, mkDir, store)
