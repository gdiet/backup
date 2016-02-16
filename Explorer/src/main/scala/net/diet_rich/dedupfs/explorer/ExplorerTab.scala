package net.diet_rich.dedupfs.explorer

import java.io.File
import java.util.Comparator
import javafx.application.Application
import javafx.collections.FXCollections
import javafx.collections.transformation.SortedList
import javafx.scene.control.TableColumn.SortType
import javafx.scene.{Scene, Parent}
import javafx.scene.control._
import javafx.scene.image.ImageView
import javafx.scene.layout.BorderPane
import javafx.stage.Stage
import javafx.util.Callback
import scala.collection.JavaConverters._

import net.diet_rich.common.init
import net.diet_rich.common.fx._

import ExplorerFile.AFile

object ExplorerFile {
  type AFile = ExplorerFile[_]
}

trait ExplorerFile[FileType <: AFile] { _: FileType =>
  def isDirectory: Boolean
  def size: Long
  def name: String
  def path: String
  def list: Seq[AFile]
  def parent: AFile
  def moveTo(other: FileType): Boolean
}

case class PhysicalExplorerFile(file: File) extends ExplorerFile[PhysicalExplorerFile] {
  override def name = file.getName
  override def path = file.getPath
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
    val explorer = new ExplorerTab(PhysicalExplorerFile(new File("E:/")))
    stage setScene new Scene(explorer.component)
    stage.show()
  }
}

class ExplorerTab(initialDir: AFile) {
  private val path = new TextField()
  private var currentDir: AFile = _
  private val files = FXCollections.observableArrayList[AFile]()
  private def reload() = files setAll currentDir.list.asJava

  runFX(cd(initialDir))

  // FIXME cd somewhere with fewer entries -> superfluous entries in table
  private def cd(newDir: AFile) = {
    currentDir = newDir
    path.setText(currentDir.path)
    reload()
  }

  val component: Parent = init(new BorderPane()) { mainPane =>
    mainPane setTop {
      init(new BorderPane()) { topPane =>
        topPane setLeft {
          init(new Button()) { upButton =>
            upButton setGraphic new ImageView(imageUp).fit(17, 17)
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
    mainPane setCenter {
      val filesSorted = new SortedList[AFile](files)
      init(new TableView[AFile](filesSorted)) { filesView =>
        filesSorted.comparatorProperty().bind(filesView.comparatorProperty())
        val iconColumn = createTableColumn[AFile]("\u25c8", {(cell, file) =>
          val image = if (file.isDirectory) imageFolder else imageFile
          cell setGraphic new ImageView(image).fit(17,17)
        })
        iconColumn setSortable false
        val nameColumn =
          createTableColumn[AFile]("Name", {(cell, file) => cell setText file.name})
          .withFileComparator((o1, o2) => o1.name.compareTo(o2.name))
        val sizeColumn =
          createTableColumn[AFile]("Size", { (cell, file) =>
            cell setText (if (file.isDirectory) "" else s"${file.size}")
          })
          .withFileComparator((o1, o2) => o1.size.compareTo(o2.size))
        filesView
          .withColumn (iconColumn)
          .withColumn (nameColumn)
          .withColumn (sizeColumn)
          .setEditable(true)
        filesView.getSortOrder.add(nameColumn)
        filesView.setRowFactory(new Callback[TableView[AFile], TableRow[AFile]] {
          override def call(param: TableView[AFile]): TableRow[AFile] = init(new TableRow[AFile]) { row =>
            row setOnMouseClicked handleDoubleClick {if (row.getItem.isDirectory) cd(row.getItem)}
          }
        })

//        filesView.getColumns.add(init(new TableColumn[ExplorerFile, String]("EName")){c =>
//          c.setCellValueFactory(new Callback[CellDataFeatures[ExplorerFile, String], ObservableValue[String]] {
//            def call(p: CellDataFeatures[ExplorerFile, String]): ObservableValue[String] =
//              new SimpleStringProperty(p.getValue.name)
//          })
//          c.setCellFactory(TextFieldTableCell.forTableColumn())
//        })
//        filesView.getColumns.add(init(new TableColumn[ExplorerFile, ExplorerFile]("E2Name")){c =>
//          c.setCellValueFactory(new Callback[CellDataFeatures[ExplorerFile, ExplorerFile], ObservableValue[ExplorerFile]] {
//            def call(p: CellDataFeatures[ExplorerFile, ExplorerFile]): ObservableValue[ExplorerFile] =
//              new ObservableValueBase[ExplorerFile] {
//                override def getValue: ExplorerFile = p.getValue
//              }
//            }
//          )
//          c.setCellFactory(TextFieldTableCell.forTableColumn(new StringConverter[ExplorerFile] {
//            override def fromString(string: String): ExplorerFile = {
//              new ExplorerFile {
//                override def size: Long = -1
//                override def name: String = string
//                override def isDirectory: Boolean = true
//              }
//            }
//            override def toString(file: ExplorerFile): String = file.name
//          }))
//          c.setOnEditCommit(new EventHandler[CellEditEvent[ExplorerFile, ExplorerFile]] {
//            override def handle(event: CellEditEvent[ExplorerFile, ExplorerFile]): Unit = {
//              println(s"edit committed: ${event.getOldValue} -> ${event.getNewValue.name}")
//            }
//          })
//        })
      }
    }
  }
}
