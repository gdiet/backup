package net.diet_rich.dedupfs.commander

import javafx.scene.Parent
import javafx.scene.control.{Button, TextField}
import javafx.scene.layout.BorderPane

import net.diet_rich.common.init
import net.diet_rich.dedupfs.commander.fx._

class FilesPane(private var file: FilesPaneItem) {
  private val pathField = new TextField()
  // validate path on "enter" or when focus is lost
  pathField setOnAction handle(validatePathField())
  pathField.focusedProperty() addListener changeListener[java.lang.Boolean]{ b => if (!b) validatePathField() }
  // reset path field style when text is typed
  pathField.textProperty() addListener changeListener[String]{ _ => pathField withoutStyle "illegalTextValue" }
  private def validatePathField(): Unit = {
    if (pathField.getText == "a")
      pathField withStyle "illegalTextValue"
    else
      pathField withoutStyle "illegalTextValue"
  }

  private val filesTable = new FilesTable(cd)

  private def cd(newDir: FilesTableItem): Unit = cd(newDir.asFilesPaneItem)
  private def cd(newDir: FilesPaneItem): Unit = { file = newDir; load() }

  private def load(): Unit = {
    filesTable.files.clear()
    filesTable.files.addAll(file.list:_*)
    filesTable.table refresh()
    pathField setText file.url
    validatePathField()
  }

  val component: Parent = init(new BorderPane()) { mainPane =>
    mainPane setTop {
      init(new BorderPane()) { topPane =>
        topPane setLeft init(new Button()) { upButton =>
          upButton withStyle "upButton"
          upButton setOnAction handle(cd(file.up))
        }
        topPane setCenter pathField
        topPane setRight init(new Button()) { reloadButton =>
          reloadButton withStyle "reloadButton"
          reloadButton setOnAction handle(load())
        }
      }
    }
    mainPane setCenter filesTable.table
  }

  load()
}
