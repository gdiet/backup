package dedup

import dedup.util.ClassLogging

import java.io.{BufferedInputStream, File, FileInputStream}
import java.util.concurrent.atomic.AtomicBoolean
import scala.io.Source
import scala.util.Using.resource

/*

######### OLD - START #########
Copy a file or a directory to the dedup file system. Source and target are mandatory, reference can be provided when
copying a directory.

If a target path element starts with "!", create it and subsequent path elements if missing.
If the target ends with "/", create the copy with in the target directory.
If the target does not end with "/", create the copy in the target directory.

copy /file /parent/dir/ -> creates file in /parent/dir, /parent/dir must be an existing directory
copy /file /parent/!dir/ -> creates file in /parent/dir, /parent must be an existing directory, dir is created on demand
copy /file /parent/dir/copy -> creates copy in /parent/dir/, /parent/dir must be an existing directory
copy /file /parent/!dir/copy -> creates copy in /parent/dir, /parent must be an existing directory, dir is created on demand

copy /dir /parent/dir/ -> creates dir in /parent/dir, /parent/dir must be an existing directory
copy /dir /parent/!dir/ -> creates dir in /parent/dir, /parent must be an existing directory, dir is created on demand
copy /dir /parent/dir/copy -> creates copy in /parent/dir/, /parent/dir must be an existing directory
copy /dir /parent/!dir/copy -> creates copy in /parent/dir, /parent must be an existing directory, dir is created on demand
######### OLD - END #########



fsc backup <source> [<source2> [<source...N>]] <target> [reference=<reference>] [forceReference=true]

Example:

fsc backup /docs /notes/\* /backup/?[yyyy]/![yyyy.MM.dd_HH.mm]/ reference=/backup/????/????.??.??_*

In the source, the wildcards "?" and "*" in the last path element are resolved to a list of matching files / directories.

`target` specifies the DedupFS directory to store the source backups in.
In `target` only the forward slash "/" a path separator. The backslash "\" is an escape character.
Everything within square brackets `[...]` is used as
[java.text.SimpleDateFormat](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/SimpleDateFormat.html)
for formatting the current date/time unless the opening square bracket is escaped with a backslash `\`.
If a path element starts with the question mark "?", the question mark is removed and the corresponding target
directory and its children are created if missing. The question mark can be escaped with a backslash `\`.
If a path element starts with the exclamation mark "!", the exclamation mark is removed. It is ensured that the corresponding
target directory does not exist, then it and its children are created. The exclamation mark can be escaped with a backslash `\`.

In `reference` only the forward slash "/" is a path separator. If a reference is specified, the tool searches for
the matching reference directory before resolving the target directory. "?" and "*" wildcards in the reference
are resolved as the alphanumerically last/highest match.

*/

object BackupTool extends ClassLogging:

  def backup(opts: Seq[(String, String)], params: List[String]): Unit =
    val (sources, target) = sourcesAndTarget(params)
    val repo              = opts.repo
    val backup            = opts.defaultFalse("dbBackup")
    val maybeReference    = opts.get("reference")
    val forceReference    = opts.defaultFalse("forceReference")
    val temp              = main.prepareTempDir(false, opts)
    val dbDir             = main.prepareDbDir(repo, backup = backup, readOnly = false)
    val settings          = server.Settings(repo, dbDir, temp, readOnly = false, copyWhenMoving = AtomicBoolean(false))

    log.info  (s"Running the backup utility")
    log.info  (s"Repository:        $repo")
    sources.foreach(source =>
      log.info(s"Backup source:     $source"))
    log.info  (s"Backup target:     DedupFS:$target")
    maybeReference.foreach { reference =>
      log.info(s"Reference pattern: DedupFS:$reference")
      log.info(s"Force reference:   $forceReference")
    }
    log.debug (s"Temp dir:          $temp")

    cache.MemCache.startupCheck()
    if backup then db.maintenance.backup(settings.dbDir)
    resource(server.Backend(settings)) { fs =>
      val maybeRefId = maybeReference.map(findReference(fs, _))
      val targetId = resolveTarget(fs, target)
    }

  def resolveTarget(fs: server.Backend, targetPath: String): Long =
    fs.pathElements(targetPath).foldLeft(("/", false, root.id)) { case ((path, createFlag, parentId), pathElement) =>
      val name = pathElement.replaceFirst("""^[!?]""", "").replace("""\""", "")
      val create = createFlag | pathElement.matches("""^[!?].*""")
      fs.child(parentId, name) match
        case None =>
          if !create then failure(s"Target path DedupFS:$path$name does not exist.")
          val dirId = fs.mkDir(parentId, name).getOrElse(failure(s"Failed to create target directory 'DedupFS:$path$name'."))
          (path + name + "/", create, dirId)
        case Some(dir: DirEntry) =>
          if pathElement.startsWith("!") then failure(s"Target path DedupFS:$path$name already exists but should not exist.")
          (path + name + "/", create, dir.id)
        case Some(_: FileEntry) =>
          failure(s"Target path DedupFS:$path$name is a file, not a directory.")
    }._3

  def findReference(fs: server.Backend, refPath: String): Long =
    val path -> id = fs.pathElements(refPath).foldLeft("/" -> root.id) { case (path -> parentId, pathElement) =>
      val pattern = """\Q""" + pathElement.replace("*", """\E.*\Q""").replace("?", """\E.\Q""") + """\E"""
      fs.children(parentId).collect {
        case dir: DirEntry if dir.name.matches(pattern) => dir
      }.sortBy(_.name).lastOption match
        case None => failure(s"Directory not found while resolving reference: DedupFS:$path$pathElement")
        case Some(dir) => path + dir.name + "/" -> dir.id
    }
    if fs.children(id).isEmpty then main.failureExit(s"Reference directory DedupFS:$path is empty.")
    log.info(s"Reference        : DedupFS:$path")
    id

  def sourcesAndTarget(params: List[String]): (List[File], String) = params.reverse match
    case Nil => failure("Source and target are missing.")
    case _ :: Nil => failure("Source or target is missing.")
    case target :: rawSources => rawSources.flatMap(resolveSource) -> resolveDateInTarget(target)

  def resolveSource(rawSource: String): List[File] =
    val replaced = rawSource.replace('\\', '/')
    if replaced.endsWith("/") then failure("""Source may not end with '/' or '\'.""")
    if replaced.contains("*") || replaced.contains("?") then
      val (path, name) = replaced.splitAt(replaced.lastIndexOf("/") + 1)
      val parentDir = File(path)
      if !parentDir.isDirectory then failure(s"Source parent $parentDir is not a directory.")
      val namePattern = name.replaceAll("""\.""", """\\.""").replaceAll("""\*""", ".*").replaceAll("""\?""", ".")
      parentDir.listFiles().filter(_.getName.matches(namePattern)).toList
    else
      val source = File(rawSource)
      if !source.canRead then failure(s"Can not read source $rawSource.")
      List(source)

  /** Replace '[...]' by formatting the contents with the SimpleDateFormat of 'now'
    * unless the opening square bracket is escaped by a backslash. */
  def resolveDateInTarget(string: String): String =
    // ([^\\])\[(.+?)]   explained:
    // ([^\\])           a character that is not a backslash as group 1
    //        \[     ]   followed by opening and later closing angle brackets
    //          (.+?)    one or more character enclosed by the angle brackets as group 2
    val regex = """([^\\])\[(.+?)]""".r
    val date = java.util.Date.from(java.time.Instant.now())
    regex.replaceAllIn(string, { m => m.group(1) + java.text.SimpleDateFormat(m.group(2)).format(date) })
      .replaceAll("""\\""", "")




  // FIXME old - remove eventually
  def backup_old(opts: Seq[(String, String)], params: List[String]): Unit =
    val (from, to, reference) = params match
      case from :: to :: reference :: Nil => (File(from), resolveDateInTarget(to), Some(reference))
      case from :: to              :: Nil => (File(from), resolveDateInTarget(to), None)
      case other => main.failureExit(s"Expected parameters '[from] [to] [optional: reference]', got '${other.mkString(" ")}'")

    val repo           = opts.repo
    val backup         = opts.defaultFalse("dbBackup")
    val forceReference = opts.defaultFalse("forceReference")
    val temp           = main.prepareTempDir(false, opts)
    val dbDir          = main.prepareDbDir(repo, backup = backup, readOnly = false)
    val settings       = server.Settings(repo, dbDir, temp, readOnly = false, copyWhenMoving = AtomicBoolean(false))
    cache.MemCache.startupCheck()
    if backup then db.maintenance.backup(settings.dbDir)

    if !from.canRead then main.failureExit(s"The backup source $from can't be read.")

    log.info (s"Running the backup utility")
    log.info (s"Repository:       $repo")
    log.info (s"Backup source:    $from")
    log.info (s"Backup target:    $to")
    log.info (s"Backup reference: $reference")
    log.info (s"Force reference:  $forceReference")
    log.debug(s"Temp dir:         $temp")

    resource(server.Backend(settings)) { fs =>
      val referenceId = reference.map(getReferenceId(fs, from, _, forceReference))
      main.failureExit("All OK, continue...")
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

  private def getReferenceId(fs: server.Backend, from: File, reference: String, forceReference: Boolean): Long =
    fs.entry(reference).map(_.id)
      .getOrElse(main.failureExit(s"Invalid reference - DedupFS:$reference does not exist."))
      .tap { ref =>
        val refListing = fs.children(ref).map(entry => s"${entry.isInstanceOf[DirEntry]}:${entry.name}").toSet
        if refListing.isEmpty then main.failureExit(s"Reference DedupFS:$reference is empty.")
        if !forceReference then
          val srcListing = from.listFiles().map(file => s"${file.isDirectory}:${file.getName}").toSet
          val intersect  = srcListing.intersect(refListing)
          if math.max(refListing.size, srcListing.size) > intersect.size * 1.6 + 1 then
            log.info(srcListing.mkString(", "))
            log.info(refListing.mkString(", "))
            main.failureExit(s"Not enough matches (${intersect.size}) between source (${srcListing.size} entries) and reference (${refListing.size} entries).")
      }
