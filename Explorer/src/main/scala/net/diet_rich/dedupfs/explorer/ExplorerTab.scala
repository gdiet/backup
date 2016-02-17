package net.diet_rich.dedupfs.explorer

import java.io.File
import javafx.application.Application
import javafx.collections.FXCollections
import javafx.collections.transformation.SortedList
import javafx.scene.control._
import javafx.scene.image.ImageView
import javafx.scene.layout.BorderPane
import javafx.scene.{Parent, Scene}
import javafx.stage.Stage
import javafx.util.Callback

import net.diet_rich.common.fx._
import net.diet_rich.common.init
import net.diet_rich.dedupfs.explorer.ExplorerFile.AFile

import scala.collection.JavaConverters._

object ExplorerFile {
  type AFile = ExplorerFile[_]
}

trait ExplorerFile[FileType <: AFile] {_: FileType =>
  def isDirectory: Boolean
  def size: Long
  def name: String
  def path: String
  def list: Seq[AFile]
  def parent: AFile
  def moveTo(other: FileType): Boolean
}

trait FileSystems {
  def fileFor(uri: String): Option[AFile]
  final def directoryFor(uri: String): Option[AFile] = fileFor(uri).filter(_.isDirectory)
}

object FileSystems extends FileSystems {
  def fileFor(uri: String): Option[AFile] = {
    val Array(protocol, path) = uri.split("\\:\\/\\/", 2)
    protocol match {
      case "file" =>
        val file = new File(path)
        if (file.exists()) Some(PhysicalExplorerFile(file)) else None
      case _ => None
    }
  }
}

case class PhysicalExplorerFile(file: File) extends ExplorerFile[PhysicalExplorerFile] {
  override def name = file.getName
  override def path = s"file://${file.getPath}"
  override def size = file.length()
  override def isDirectory = file.isDirectory
  override def list = file.listFiles().toSeq map PhysicalExplorerFile
  override def parent = PhysicalExplorerFile(Option(file.getParentFile) getOrElse file)
  override def moveTo(other: PhysicalExplorerFile) = file.renameTo(other.file)
}

object ExplorerApp extends App {
  Application launch classOf[ExplorerApp]
}

class ExplorerApp extends Application {
  override def start(stage: Stage): Unit = {
    val parent = new Commander(FileSystems, PhysicalExplorerFile(new File("E:/")))
    stage setScene net.diet_rich.common.init(new Scene(parent.component)) { _.getStylesheets.add("style.css") }
    stage.show()
  }
}

class Commander(fileSystems: FileSystems, initialDir: AFile) {
  private val left  = new ExplorerTab(fileSystems, initialDir)
  private val right = new ExplorerTab(fileSystems, initialDir)

  val component: Parent = init(new SplitPane()) { splitPane =>
    splitPane.getItems.addAll(left.component, right.component)
  }
}

class ExplorerTab(fileSystems: FileSystems, initialDir: AFile) {
  private var currentDir: AFile = _
  private val files = FXCollections.observableArrayList[AFile]()
  private val path: TextField = init(new TextField()) {
    _.setOnAction(handleAction {
      fileSystems.directoryFor(path.getText) foreach cd
    })
  }

  private def reload() = {
    files setAll currentDir.list.asJava
    tableView refresh()
  }

  runFX(cd(initialDir))

  private def cd(newDir: AFile): Unit = {
    currentDir = newDir
    path.setText(currentDir.path)
    reload()
  }

  private val filesSorted = new SortedList[AFile](files)
  private val tableView = init(new TableView[AFile](filesSorted)) { filesView =>
    filesSorted.comparatorProperty().bind(filesView.comparatorProperty())
    val iconColumn = createTableColumn[AFile]("\u25c8", { (cell, file) =>
      val image = if (file.isDirectory) imageFolder else imageFile
      cell setGraphic new ImageView(image).fit(17, 17)
    })
    iconColumn setSortable false
    val nameColumn =
      createTableColumn[AFile]("Name", { (cell, file) => cell setText file.name })
        .withFileComparator((o1, o2) => o1.name.compareTo(o2.name))
    val sizeColumn =
      createTableColumn[AFile]("Size", { (cell, file) =>
        cell setText (if (file.isDirectory) "" else s"${file.size}")
      })
        .withFileComparator((o1, o2) => o1.size.compareTo(o2.size))
    filesView
      .withColumn(iconColumn)
      .withColumn(nameColumn)
      .withColumn(sizeColumn)
      .setEditable(true)
    filesView.getSortOrder.add(nameColumn)
    filesView.setRowFactory(new Callback[TableView[AFile], TableRow[AFile]] {
      override def call(param: TableView[AFile]): TableRow[AFile] = init(new TableRow[AFile]) { row =>
        row setOnMouseClicked handleDoubleClick {
          if (row.getItem.isDirectory) cd(row.getItem)
        }
      }
    })
  }

  val component: Parent = init(new BorderPane()) { mainPane =>
    mainPane setTop {
      init(new BorderPane()) { topPane =>
        topPane setLeft {
          init(new Button()) { upButton =>
            upButton setGraphic new ImageView().fit(17, 17).styled("up-button-image")
            upButton setOnAction handleAction(cd(currentDir.parent))
          }
        }
        topPane setCenter path
        topPane setRight {
          init(new Button()) { reloadButton =>
            reloadButton setGraphic new ImageView(imageReload).fit(17, 17)
            reloadButton setOnAction handleAction(reload())
          }
        }
      }
    }
    mainPane setCenter tableView
  }
}
