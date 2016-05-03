package net.diet_rich.dedupfs.commander

import javafx.scene.Parent
import javafx.scene.control.{Button, TextField}
import javafx.scene.layout.BorderPane

import net.diet_rich.common.init
import net.diet_rich.dedupfs.commander.fx._

class FilesPane(private var file: FilesPaneItem) {
  private val pathField = new TextField()
  private val filesTable = new FilesTable()
  def load(): Unit = {
    filesTable.files.clear()
    filesTable.files.addAll(file.list:_*)
    filesTable.table refresh()
    pathField setText file.url
  }
  val component: Parent = init(new BorderPane()) { mainPane =>
    mainPane setTop {
      init(new BorderPane()) { topPane =>
        topPane setLeft init(new Button()) { upButton =>
          upButton.getStyleClass add "upButton"
          upButton setOnAction handle {
            file = file.up
            load()
          }
        }
        topPane setCenter pathField
        topPane setRight init(new Button()) { reloadButton =>
          reloadButton.getStyleClass add "reloadButton"
          reloadButton setOnAction handle(load())
        }
      }
    }
    mainPane setCenter filesTable.table
  }

  load()
}
