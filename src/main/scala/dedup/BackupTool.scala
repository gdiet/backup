package dedup

import dedup.util.ClassLogging

import java.io.{BufferedInputStream, File, FileInputStream}
import java.util.concurrent.atomic.AtomicBoolean
import scala.io.Source
import scala.util.Using.resource

object BackupTool extends ClassLogging:

  /** Replace '[...]' by formatting the contents with the SimpleDateFormat of 'now'
    * unless the opening square bracket is escaped by a backslash.  */
  private def insertDate(string: String): String =
    // ([^\\])\[(.+?)]   explained:
    // ([^\\])           a character that is not a backslash as group 1
    //        \[     ]   followed by opening and later closing angle brackets
    //          (.+?)    one or more character enclosed by the angle brackets as group 2
    val regex = """([^\\])\[(.+?)]""".r
    val date = java.util.Date.from(java.time.Instant.now())
    regex.replaceAllIn(string, { m => m.group(1) + java.text.SimpleDateFormat(m.group(2)).format(date) })
      .replaceAll("""\\""", "")

  def backup(opts: Seq[(String, String)], params: List[String]): Unit = try {
    val (from, to, reference) = params match
      case from :: to :: reference :: Nil => (File(from), insertDate(to), Some(reference))
      case from :: to              :: Nil => (File(from), insertDate(to), None)
      case other => main.failureExit(s"Expected parameters '[from] [to] [optional: reference]', got '${other.mkString(" ")}'")

    val repo     = opts.repo
    val backup   = opts.defaultFalse("dbBackup")
    val temp     = main.prepareTempDir(false, opts)
    val dbDir    = main.prepareDbDir(repo, backup = backup, readOnly = false)
    val settings = server.Settings(repo, dbDir, temp, readOnly = false, copyWhenMoving = AtomicBoolean(false))
    cache.MemCache.startupCheck()
    if backup then db.maintenance.backup(settings.dbDir)

    if !from.canRead then main.failureExit(s"The backup source $from can't be read.")

    log.info (s"Running the backup utility")
    log.info (s"Repository:       $repo")
    log.info (s"Backup source:    $from")
    log.info (s"Backup target:    $to")
    log.info (s"Backup reference: $reference  --  No functionality implemented yet")
    log.debug(s"Temp dir:         $temp")

    resource(server.Backend(settings)) { fs =>
      val (targetPath, targetName) = fs.pathElements(to).pipe(target =>
        target.dropRight(1) -> target.lastOption.getOrElse(main.failureExit(s"Invalid target: No file name in '$to'."))
      )
      def invalidParentForLog = s"Invalid target - the target's parent DedupFS:/${targetPath.mkString("/")}"
      val targetParent = fs.entry(targetPath) match
        case Some(dir: DirEntry) => dir
        case Some(_: FileEntry)  => main.failureExit(s"$invalidParentForLog points to a file.")
        case None                => main.failureExit(s"$invalidParentForLog does not exist.")
      val targetId = fs.mkDir(targetParent.id, targetName).getOrElse(
        main.failureExit(s"Invalid target - DedupFS:$to already exists.")
      )
      log.info(s"Created 'DedupFS:$to'.")

      def mkDir(parent: Long, name: String): Long =
        def nameToUse = if name.isEmpty then "[drive]" else name
        fs.mkDir(parent, nameToUse).getOrElse(main.failureExit(s"Unexpected name conflict creating directory $nameToUse."))

      def store(parent: Long, file: File): Unit =
        val id = fs.createAndOpen(parent, file.getName, Time(file.lastModified()))
          .getOrElse(main.failureExit(s"Unexpected name conflict creating backup file for $file."))
        resource(BufferedInputStream(FileInputStream(file))) { in =>
          var position = 0L
          val data = Iterator.continually(in.readNBytes(memChunk))
            .takeWhile(_.nonEmpty)
            .map { chunk =>
              val currentPosition = position
              position += chunk.length
              currentPosition -> chunk
            }
          fs.write(id, data)
        }
        fs.release(id)
        log.info(s"Stored: $file")

      processRecurse(Seq((targetId, Seq(), "/", from)), mkDir, store)
    }
  } catch { case t: Throwable => log.error("Uncaught exception:", t); throw t }

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
          val add = Option(source.listFiles()).toSeq.flatten.map((dir, ignore, sourcePath, _))
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
            ignoreRules
              .map(_.replaceAll("\\?", "\\\\E.\\\\Q").replaceAll("\\*", "\\\\E.*\\\\Q"))
              .map("\\Q" + _ + "\\E")
          val dir = mkDir(parent, source.getName)
          val add = Option(source.listFiles()).toSeq.flatten.map((dir, ignore ++ additionalIgnore, sourcePath, _))
          processRecurse(remaining ++ add, mkDir, store)
