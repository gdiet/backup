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
  if (startId == 0) checkTables
  progressBar write "checking datastore..."
  checkData
  progressBar write "shutting down..."
  fileSystem teardown()
  progressBar write "finished."

  def runLater(f: => Unit) = SwingUtilities invokeLater new Runnable {override def run() = {f}}

  def checkTables = {
    // TODO
  }

  def checkData = {
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
    def error(string: String) = {
      println(string)
      runLater {
        progressBar.setBackground(Color.orange)
        progressBar.setForeground(Color.red)
      }
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
