package net.diet_rich.dedupfs.explorer.filesPane

import javafx.scene.Parent
import javafx.scene.control.{Button, TextField}
import javafx.scene.layout.BorderPane

import net.diet_rich.common.init
import net.diet_rich.common.fx._
import net.diet_rich.dedupfs.explorer.imageView
import net.diet_rich.dedupfs.explorer.filesPane.filesTable.{FilesTableItem, FilesTable}

class FilesPane(registry: FileSystemRegistry, initialUrl: String) {
  private var directory_ = registry get initialUrl
  def directory: Option[FilesPaneDirectory] = directory_

  private val pathField = new TextField(initialUrl)
  // validate path on "enter"
  pathField setOnAction handle(cd())
  // validate path when focus is lost
  pathField.focusedProperty() addListener changeListener { b: java.lang.Boolean => if (!b) cd() }
  // reset path field style when text is typed
  pathField.textProperty() addListener changeListener[String]{ _ => pathField withoutStyle "illegalTextValue" }

  def setActive(active: Boolean) =
    if(active) pathField withStyle "activePanel"
    else pathField withoutStyle "activePanel"

  private def cd(): Unit = cd(registry get pathField.getText)
  private def cd(newDir: Option[FilesPaneDirectory]): Unit = { directory_ = newDir; refresh() }

  private val filesTable = new FilesTable(file => cd(file.asFilesPaneItem))
  def renameSingleSelection(): Unit = filesTable renameSingleSelection()
  def selectedFiles: Seq[FilesTableItem] = filesTable.selectedFiles

  def refresh(source: Option[FilesPaneDirectory]): Unit = if (source == directory) refresh()
  def refresh(): Unit = {
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
          reloadButton setOnAction handle(refresh())
        }
      }
    }
    mainPane setCenter filesTable.table
  }

  refresh()
}
