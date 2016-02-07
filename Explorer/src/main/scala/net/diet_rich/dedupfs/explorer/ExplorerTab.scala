package net.diet_rich.dedupfs.explorer

import java.io.File
import javafx.application.Application
import javafx.collections.FXCollections
import javafx.scene.{Scene, Parent}
import javafx.scene.control._
import javafx.scene.image.ImageView
import javafx.scene.layout.BorderPane
import javafx.stage.Stage

import net.diet_rich.common.init

trait ExplorerFile {
  def isDirectory: Boolean
  def size: Long
  def name: String
}

case class ExplorerPhysicalFile(file: File) extends ExplorerFile {
  override def name = file.getName
  override def size = file.length()
  override def isDirectory = file.isDirectory
}

object ExplorerApp extends App {
  Application launch classOf[ExplorerApp]
}

class ExplorerApp extends Application {
  override def start(stage: Stage): Unit = {
    val explorer = new ExplorerTab
    explorer.files.setAll(ExplorerPhysicalFile(new File("C:/")), ExplorerPhysicalFile(new File("C:/someFile")))
    stage setScene new Scene(explorer.component)
    stage.show()
    new Thread() {
      override def run() = {
        Thread.sleep(3000)
        runFX {
          println("adding now...")
          explorer.files.add(ExplorerPhysicalFile(new File("C:/Windows")))
        }
      }
    }.start()
  }
}

class ExplorerTab {
  val up = new Button()
  val path = new TextField()
  val reload = new Button()
  val files = FXCollections.observableList[ExplorerFile](new java.util.ArrayList[ExplorerFile]())

  val component: Parent = init(new BorderPane()) { mainPane =>
    mainPane setTop {
      init(new BorderPane()) { topPane =>
        topPane setLeft {
          init(up) { up =>
            up setGraphic new ImageView(imageUp).fit(17, 17)
          }
        }
        topPane setCenter path
        topPane setRight {
          init(reload) { reload =>
            reload setGraphic new ImageView(imageReload).fit(17, 17)
          }
        }
      }
    }
    mainPane setCenter {
      init(new TableView[ExplorerFile](files)) { filesView =>
        filesView setEditable true
        filesView.withColumn ("", { (cell, file) =>
          val image = if (file.isDirectory) imageFolder else imageFile
          cell setGraphic new ImageView(image).fit(17,17)
        })
        filesView.withColumn ("Name", {(cell, file) => cell setText file.name})
      }
    }
  }
}
