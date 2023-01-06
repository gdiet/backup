package dedup

import java.io.{BufferedInputStream, File, FileInputStream}
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Using.resource

object BackupTool extends dedup.util.ClassLogging:

  def backup(opts: Seq[(String, String)], params: List[String]): Unit =
    log.info  (s"Running the backup utility")

    val (sources, target) = sourcesAndTarget(params)
    val repo              = opts.repo
    val backup            = opts.defaultFalse("dbBackup")
    val maybeReference    = opts.get("reference")
    val forceReference    = opts.defaultFalse("forceReference")
    val temp              = main.prepareTempDir(false, opts)
    val dbDir             = main.prepareDbDir(repo, backup = backup, readOnly = false)
    val settings          = server.Settings(repo, dbDir, temp, readOnly = false, copyWhenMoving = AtomicBoolean(false))

    cache.MemCache.startupCheck()
    if backup then db.maintenance.backup(settings.dbDir)
    resource(server.Backend(settings)) { fs =>
      log  .info(s"Repository:        $repo")
      sources.foreach(source =>
        log.info(s"Backup source:     $source"))
      log  .info(s"Backup target:     DedupFS:$target")
      maybeReference.foreach { reference =>
        log.info(s"Reference pattern: DedupFS:$reference")
        log.info(s"Force reference:   $forceReference")
      }
      log .debug(s"Temp dir:          $temp")

      val maybeRefId = maybeReference.map(findReferenceId(fs, _))
      if !forceReference then maybeRefId.foreach(validateReference(fs, sources, _))
      val targetId = resolveTargetId(fs, target)

      val bytesStored = sources.map(source => process(fs, source, Seq(), targetId, maybeRefId)).sum
      log.info(s"Finished storing in total ${readableBytes(bytesStored)}.")
    }

  /** @param ignore The rules which files or directories to ignore. Each rule is a [[List]][String]. If a rule consists
    *               of a single element, this element is matched in the current context against directory names if the
    *               element terminates with `/`, otherwise against file names. If a rule consists of multiple elements,
    *               its tail is handed over to the processing context of all subdirectories matching the head.
    * @return The number of bytes processed. */
  def process(fs: server.Backend, source: File, ignore: Seq[List[String]], targetId: Long, maybeRefId: Option[Long]): Long =
    if ignore.flatten.nonEmpty then log.trace(s"Ignore rules for '$source': $ignore")
    val name = source.getName
    if source.isFile then
      if ignore.collect { case List(rule) => rule }.exists(name.matches) then
        log.trace(s"Ignored file due to rules: $source")
        0L
      else
        log.trace(s"Processing file: $source")
        val fileReference = maybeRefId.flatMap(fs.child(_, name)).collect { case f: FileEntry => f }
        processFile(fs, source, targetId, fileReference)
    else if ignore.flatMap(_.headOption).exists(s"$name/".matches) then
      log.trace(s"Ignored directory due to rules: $source")
      0L
    else
      val ignoreFile = File(source, ".backupignore")
      if ignoreFile.isFile && ignoreFile.length() == 0 then
        log.trace(s"Ignored directory due to ignore file: $source")
        0L
      else
        log.trace(s"Processing directory: $source")
        val updatedIgnores =
          ignore.filter(_.headOption.exists(name.matches)).map(_.tail).filterNot(_.isEmpty)
        val newIgnores =
          if ignoreFile.isFile && ignoreFile.canRead then
            resource(scala.io.Source.fromFile(ignoreFile))(
              _.getLines().map(_.trim).filterNot(_.startsWith("#")).filter(_.nonEmpty).toSeq
            ).pipe(deriveIgnoreRules).tap { i => log.debug(s"In directory '$source' read ignore rules: $i") }
          else Seq()
        val maybeDirId = fs.child(targetId, name) match
          case Some(dir: DirEntry) => Some(dir.id)
          case other =>
            if other.isDefined && !fs.deleteChildless(other.get) then
              log.warn(s"Could not replace target file for '$source'."); None
            else fs.mkDir(targetId, name)
              .tap { id => if id.isEmpty then log.warn(s"Could not create target directory for '$source'.") }
        maybeDirId match
          case None => 0L
          case Some(dirId) =>
            val newReference = maybeRefId.flatMap(fs.child(_, name)).map(_.id)
            source.listFiles().map(child => process(fs, child, updatedIgnores ++ newIgnores, dirId, newReference)).sum

  /** @return The ignore rules in their internal format. */
  def deriveIgnoreRules(ruleStrings: Seq[String]): Seq[List[String]] =
    ruleStrings.flatMap { line =>
      val parts = line.split("/").map(createWildcardPattern)
      if parts.isEmpty then None else
        if line.endsWith("/") then parts.update(parts.length - 1, parts(parts.length - 1) + "/")
        Some(parts.toList)
    }

  /** @return A [[java.util.regex.Pattern]] string for wildcard `*` and `?` matching. */
  def createWildcardPattern(base: String): String =
    s"\\Q${base.replace("*", "\\E.*\\Q").replace("?", "\\E.\\Q")}\\E"
  
  def processFile(fs: server.Backend, source: File, targetId: Long, maybeReference: Option[FileEntry]): Long =
    // Check for a reference match
    val matchingReference = maybeReference.filter { file =>
      val timeMatches = file.time == Time(source.lastModified())
      val sizeMatches = fs.size(file) == source.length()
      log.debug(s"Matching to reference time ($timeMatches) and size ($sizeMatches): $source")
      timeMatches && sizeMatches
    }
    processFile2(fs, source, targetId, matchingReference)

  def processFile2(fs: server.Backend, source: File, targetId: Long, matchingReference: Option[FileEntry]): Long =
    // Make sure in the target there is no directory where the file will be created.
    fs.child(targetId, source.getName) match {
      case Some(fileEntry: FileEntry) => matchingReference match
        case None => updateTargetFile(fs, source, fileEntry)
        case Some(reference) => updateTargetFile(fs, source, fileEntry, reference)
      case Some(dirEntry: DirEntry) =>
        def delete(treeEntry: TreeEntry): Boolean = fs.children(treeEntry.id).forall(delete) && fs.deleteChildless(treeEntry)
        if delete(dirEntry) then matchingReference match
          case None => createTargetFile(fs, source, targetId)
          case Some(reference) => createTargetFile(fs, source, targetId, reference)
        else { log.warn(s"Could not replace target directory for file '$source'."); 0L }
      case None => matchingReference match
        case None => createTargetFile(fs, source, targetId)
        case Some(reference) => createTargetFile(fs, source, targetId, reference)
    }

  def createTargetFile(fs: server.Backend, source: File, targetId: Long, matchingReference: FileEntry): Long =
    if fs.copyFile(matchingReference, targetId, source.getName) then source.length()
    else
      log.warn(s"Could not copy reference for file '$source'.")
      0L

  def updateTargetFile(fs: server.Backend, source: File, target: FileEntry, matchingReference: FileEntry): Long =
    if target.id == matchingReference.id then
      source.length()
    else
      if fs.deleteChildless(target) then
        fs.copyFile(matchingReference, target.parentId, source.getName)
        source.length()
      else
        log.warn(s"Could not replace target file for file '$source'.")
        0L

  def createTargetFile(fs: server.Backend, source: File, targetId: Long): Long =
    fs.createAndOpen(targetId, source.getName, Time(source.lastModified())) match
      case None =>
        log.warn(s"Could not create target file for file '$source'.")
        0L
      case Some(id) =>
        writeTargetFile(fs, source, id)

  def updateTargetFile(fs: server.Backend, source: File, target: FileEntry): Long =
    fs.setTime(target.id, source.lastModified())
    fs.open(target)
    writeTargetFile(fs, source, target.id)

  def writeTargetFile(fs: server.Backend, source: File, targetId: Long): Long =
    resource(BufferedInputStream(FileInputStream(source))) { in =>
      var position = 0L
      val data = Iterator.continually(in.readNBytes(memChunk))
        .takeWhile(_.nonEmpty)
        .map { chunk =>
          val currentPosition = position
          position += chunk.length
          currentPosition -> chunk
        }
      fs.write(targetId, data)
    }
    fs.release(targetId)
    log.info(s"Stored: $source")
    source.length()

  def validateReference(fs: server.Backend, sources: List[File], refId: Long): Unit =
    def comp1(entry: TreeEntry) = if entry.isInstanceOf[DirEntry] then entry.name   else ":" + entry.name
    def comp2(file : File     ) = if file.isDirectory             then file.getName else ":" + file.getName
    val referenceBase = fs.children(refId).map(comp1).toSet
    val sourcesBase = sources.map(comp2).toSet
    val referenceListing = fs.children(refId).flatMap {
      case file: FileEntry => List(":" + file.name)
      case dir: DirEntry if sourcesBase.contains(dir.name) =>
        List(dir.name) ++ fs.children(dir.id).map(dir.name + "/" + comp1(_))
      case dir: DirEntry => List(dir.name)
    }.sorted
    val sourceListing = sources.flatMap {
      case file if !file.isDirectory => List(":" + file.getName)
      case dir if referenceBase.contains(dir.getName) =>
        List(dir.getName) ++ dir.listFiles().map(dir.getName + "/" + comp2(_))
      case dir => List(dir.getName)
    }.sorted
    sourceListing   .foreach(entry => log.debug(s"Source listing entry   : $entry"))
    referenceListing.foreach(entry => log.debug(s"Reference listing entry: $entry"))
    val intersectCount = sourceListing.toSet.intersect(referenceListing.toSet).size
    if math.max(referenceListing.size, sourceListing.size) > intersectCount * 1.6 + 1 then
      main.failureExit(s"Not enough matches ($intersectCount) between source (${sourceListing.size} entries) and reference (${referenceListing.size} entries).")
    log.info(s"Reference validation OK, $intersectCount matches between source (${sourceListing.size} entries) and reference (${referenceListing.size} entries).")

  def resolveTargetId(fs: server.Backend, targetPath: String): Long =
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

  def findReferenceId(fs: server.Backend, refPath: String): Long =
    val path -> id = fs.pathElements(refPath).foldLeft("/" -> root.id) { case (path -> parentId, pathElement) =>
      val pattern = createWildcardPattern(pathElement)
      fs.children(parentId).collect {
        case dir: DirEntry if dir.name.matches(pattern) => dir
      }.sortBy(_.name).lastOption match
        case None => failure(s"Directory not found while resolving reference: DedupFS:$path$pathElement")
        case Some(dir) => path + dir.name + "/" -> dir.id
    }
    if fs.children(id).isEmpty then main.failureExit(s"Reference directory DedupFS:$path is empty.")
    log.info(s"Reference:         DedupFS:$path")
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
    string.split("/").filterNot(_.isEmpty).map(resolveDateInTargetPathElement).mkString("/", "/", "")

  /** Replace '[...]' by formatting the contents with the SimpleDateFormat of 'now'
    * unless the opening square bracket is escaped by a backslash. */
  def resolveDateInTargetPathElement(string: String): String =
    // ([^\\])\[(.+?)]   explained:
    // ([^\\])           a character that is not a backslash as group 1
    //        \[     ]   followed by opening and later closing angle brackets
    //          (.+?)    one or more character enclosed by the angle brackets as group 2
    val regex = """([^\\])\[(.+?)]""".r
    val date = java.util.Date.from(java.time.Instant.now())
    regex.replaceAllIn(string, { m => m.group(1) + java.text.SimpleDateFormat(m.group(2)).format(date) })
      .replaceAll("""\\\[""", "[")
