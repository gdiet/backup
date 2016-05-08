package net.diet_rich.dedupfs.commander

import javafx.scene.Parent
import javafx.scene.control.{Button, TextField}
import javafx.scene.layout.BorderPane

import net.diet_rich.common.init
import net.diet_rich.dedupfs.commander.fx._

class FilesPane(initialUrl: String) {
  def setActive(active: Boolean) =
    if(active) pathField withStyle "activePanel"
    else pathField withoutStyle "activePanel"

  private var directory = FileSystemRegistry get initialUrl
  private val pathField = new TextField()
  // validate path on "enter" or when focus is lost
  pathField setOnAction handle(cd())
  pathField.focusedProperty() addListener changeListener { b: java.lang.Boolean => if (!b) cd() }
  // reset path field style when text is typed
  pathField.textProperty() addListener changeListener[String]{ _ => pathField withoutStyle "illegalTextValue" }

  private def cd(): Unit = cd(FileSystemRegistry.get(pathField.getText))
  private def cd(newDir: Option[FilesPaneDirectory]): Unit = { directory = newDir; load() }

  private val filesTable = new FilesTable(file => cd(file.asFilesPaneItem))

  private def load(): Unit = {
    filesTable.files.clear()
    directory match {
      case Some(dir) =>
        filesTable.files.addAll(dir.list:_*)
        pathField setText dir.url
        pathField withoutStyle "illegalTextValue"
      case None =>
        pathField withStyle "illegalTextValue"
    }
    filesTable.table refresh()
  }

  val component: Parent = init(new BorderPane()) { mainPane =>
    mainPane setTop {
      init(new BorderPane()) { topPane =>
        topPane setLeft init(new Button("", imageView("button.up"))) { upButton =>
          upButton setOnAction handle(cd(directory map (_.up)))
        }
        topPane setCenter pathField
        topPane setRight init(new Button("", imageView("button.reload"))) { reloadButton =>
          reloadButton setOnAction handle(load())
        }
      }
    }
    mainPane setCenter filesTable.table
  }

  load()
}
