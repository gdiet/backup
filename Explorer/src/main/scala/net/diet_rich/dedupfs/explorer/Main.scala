package net.diet_rich.dedupfs.explorer

import java.util.concurrent.atomic.AtomicInteger
import javafx.application.Application
import javafx.beans.property.{SimpleStringProperty, ReadOnlyIntegerWrapper, ReadOnlyStringWrapper}
import javafx.beans.value.ObservableValue
import javafx.collections.FXCollections
import javafx.scene.Scene
import javafx.scene.control.TableColumn.CellDataFeatures
import javafx.scene.control._
import javafx.scene.control.cell.TextFieldTableCell
import javafx.scene.image.{ImageView, Image}
import javafx.scene.layout.BorderPane
import javafx.stage.Stage
import javafx.util.Callback
import scala.collection.JavaConverters._

import net.diet_rich.common.{Arguments, Logging}
import net.diet_rich.dedupfs.StoreMethod

object Main extends App with Logging {
  val arguments = new Arguments(args, 1)
  val List(repoPath) = arguments.parameters
  val storeMethod = arguments optional "storeMethod" map StoreMethod.named getOrElse StoreMethod.STORE
  arguments withSettingsChecked { Application launch classOf[Main] }
}

class Main extends Application {
  override def start(stage: Stage): Unit = {
    val currentDir = new TextField("C:/directory/subdir/current")
    val imageUp = new ImageView(new Image("com.modernuiicons/appbar.chevron.up.png"))
    imageUp.setFitHeight(17)
    imageUp.setFitWidth(17)
    val up = new Button("", imageUp)
    val imageReload = new ImageView(new Image("com.modernuiicons/appbar.refresh.png"))
    imageReload.setFitHeight(17)
    imageReload.setFitWidth(17)
    val reload = new Button("", imageReload)

    val pathPane = new BorderPane()
    pathPane.setLeft(up)
    pathPane.setCenter(currentDir)
    pathPane.setRight(reload)

    case class FileEntry(name: String, size: Long)

    val filesList = FXCollections.observableList(List(
      FileEntry("file 1 with an impressive long name", 10),
      FileEntry("file 2", 20)
    ).asJava)

    val n = new AtomicInteger(1)

    val filesView = new TableView[FileEntry](filesList)
    filesView.setEditable(true)
    val iconColumn = new TableColumn[FileEntry, java.lang.Number]("Icon")
    iconColumn.setCellValueFactory(new Callback[CellDataFeatures[FileEntry, java.lang.Number], ObservableValue[java.lang.Number]] {
      def call(p: CellDataFeatures[FileEntry, Number]): ObservableValue[java.lang.Number] = {
        new ReadOnlyIntegerWrapper(n.incrementAndGet())
      }
    })
    iconColumn.setCellFactory(new Callback[TableColumn[FileEntry, Number], TableCell[FileEntry, Number]] {
      override def call(param: TableColumn[FileEntry, Number]): TableCell[FileEntry, Number] = new TableCell[FileEntry, Number]() {
        override def updateItem(item: Number, empty: Boolean): Unit = {
          if (!empty) {
            val image = if (item.intValue() % 2 == 0) imageFile else imageFolder
            setGraphic(new ImageView(image).fit(17,17))
          }
        }
      }
    })
    val nameColumn = new TableColumn[FileEntry, String]("Name")
    nameColumn.setCellValueFactory(new Callback[CellDataFeatures[FileEntry, String], ObservableValue[String]] {
      def call(p: CellDataFeatures[FileEntry, String]): ObservableValue[String] =
        new SimpleStringProperty(p.getValue.name)
    })
    nameColumn.setCellFactory(TextFieldTableCell.forTableColumn())
//    nameColumn.setEditable(true)
    val sizeColumn = new TableColumn[FileEntry, String]("Size")
    sizeColumn.setCellValueFactory(new Callback[CellDataFeatures[FileEntry, String], ObservableValue[String]] {
      def call(p: CellDataFeatures[FileEntry, String]): ObservableValue[String] =
        new ReadOnlyStringWrapper(p.getValue.size.toString)
    })
    filesView.getColumns.add(iconColumn)
    filesView.getColumns.add(nameColumn)
    filesView.getColumns.add(sizeColumn)

    val borderPane = new BorderPane()
    borderPane.setTop(pathPane)
    borderPane.setCenter(filesView)

    val scene = new Scene(borderPane)

    stage setScene scene
    stage setTitle "Dedup file system explorer"
    stage show()
  }
}
