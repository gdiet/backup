package net.diet_rich.dedup.core

import java.awt.Color
import javax.swing.SwingUtilities

import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.util.{init, ConsoleApp}

import scala.slick.jdbc.StaticQuery

object CheckRepositoryApp extends ConsoleApp {
  checkUsage("parameters: <repository path> [continueAt:(dataid)]")
  val startId = option("continueAt:", "0").toLong

  import scala.language.reflectiveCalls
  val progressBar = {
    import javax.swing._
    val frame = new JFrame("Repository check progress")
    init(new JProgressBar() {
      def write(string: String) = runLater { setString(string) }
    }) { bar =>
      bar setString ""
      bar setStringPainted true
      frame.getContentPane add bar
      frame setDefaultCloseOperation JFrame.EXIT_ON_CLOSE
      frame setSize (450, 80)
      frame setLocationRelativeTo null
      frame setVisible true
    }
  }

  progressBar write "initializing backup file system..."
  val fileSystem = init(Repository.diagnosticFileSystem(repositoryDirectory, readonly = true))(_ setup())
  import fileSystem.session

  progressBar write "checking database..."
  if (startId == 0) checkTables()
  progressBar write "checking datastore..."
  checkData()
  progressBar write "shutting down..."
  fileSystem teardown()
  progressBar write "finished."

  def runLater(f: => Unit) = SwingUtilities invokeLater new Runnable {override def run() = {f}}

  def error(string: String) = {
    println(string)
    runLater {
      progressBar.setBackground(Color.orange)
      progressBar.setForeground(Color.red)
    }
  }

  def cyclicParentRelations(treeEntries: Iterator[TreeEntry]): List[TreeEntry] =
    treeEntries.foldLeft(List.empty[TreeEntry]) {
      case (unresolved, entry) if entry.parent.value >= entry.id.value || unresolved.exists(_.id == entry.parent) =>
        entry :: unresolved
      case (unresolved, entry) =>
        def clearUnresoved(node: TreeEntry, unresolved: List[TreeEntry]): List[TreeEntry] = {
          val (children, other) = unresolved.partition(_.parent == node.id)
          children.foldLeft(other) { case (rest, child) => clearUnresoved(child, rest) }
        }
        clearUnresoved(entry, unresolved)
    }

  def checkTables() = {
    val (rootList, nodesWithoutParent) = StaticQuery.queryNA[Long](
      "SELECT ta.id FROM TreeEntries ta LEFT JOIN TreeEntries tb ON ta.parent = tb.id WHERE tb.id IS NULL;"
    ).list.partition(_ == 0L)
    if (rootList != List(0L)) error("tree root 0 not found when looking for entries without parent")
    if (nodesWithoutParent.nonEmpty) error(s"tree nodes without parent: $nodesWithoutParent")

    val missingDataEntries = StaticQuery.queryNA[(Long, Long)](
      "SELECT t.id, t.dataid FROM TreeEntries t LEFT JOIN DataEntries d ON t.dataid = d.id WHERE d.id IS NULL;"
    ).list
    if (missingDataEntries.nonEmpty) error(s"missing data entries (tree entry id / data id): $nodesWithoutParent")

    val (emptyEntry, missingByteStoreEntries) = StaticQuery.queryNA[Long](
      "SELECT d.id FROM DataEntries d LEFT JOIN ByteStore b ON d.id = b.dataid WHERE b.dataid IS NULL;"
    ).list.partition(_ == 0L)
    if (emptyEntry != List(0L)) error("empty data entry 0 not found when looking for entries without byte store entries")
    if (missingByteStoreEntries.nonEmpty) error(s"data entries without byte store entries: $missingByteStoreEntries")

    val treeEntries = sql.TableQueries.sortedTreeEntriesQuery.iterator
    val cyclicRelations = cyclicParentRelations(treeEntries)
    if (cyclicRelations.nonEmpty) error(s"tree entries with cyclic relations: $cyclicRelations")
  }

  def checkData() = {
    val (maxId, dataEntries) = {
      import sql.TableQueries._
      (StaticQuery.queryNA[Long]("SELECT MAX(id) FROM DataEntries;").first,
        StaticQuery.queryNA[DataEntry](s"$selectFromDataEntries WHERE id >= $startId;"))
    }
    val nextId = new java.util.concurrent.atomic.AtomicLong(startId)
    val shutdownHook = sys.addShutdownHook {
      println(s"shutting down... next id to check: ${nextId.get}")
      fileSystem.teardown()
    }
    dataEntries.foreach { entry =>
      nextId set entry.id.value
      runLater {
        progressBar setValue (entry.id.value.toDouble / maxId * 100).toInt
        progressBar setString s"${entry.id.value} / $maxId"
      }
      val data = fileSystem.read(entry.id)
      val (hash, size, print) = Hash.calculate(fileSystem.storeSettings hashAlgorithm, data, { data =>
        val printHeader = data.foldLeft(Bytes.EMPTY) {
          case (header, _) if header.length == FileSystem.PRINTSIZE => header
          case (header, chunk) =>
            val chunkSize = math.min(FileSystem.PRINTSIZE - header.length, chunk.length)
            init(Bytes.zero(header.length + chunkSize)) { newHeader =>
              System arraycopy (header.data, header.offset, newHeader.data, 0, header.length)
              System arraycopy (chunk.data, chunk.offset, newHeader.data, header.length, chunkSize)
            }
        }
        Print(printHeader)
      })
      if (entry.size != size) error(s"data entry ${entry.id}: size $size does not match size in database ${entry.size}")
      if (entry.hash !== hash) error(s"data entry ${entry.id}: hash does not match hash in database")
      if (entry.print != print) error(s"data entry ${entry.id}: print $print does not match print in database ${entry.print}")
    }
    shutdownHook remove()
  }
}
