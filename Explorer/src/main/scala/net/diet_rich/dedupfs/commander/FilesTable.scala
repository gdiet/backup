package net.diet_rich.dedupfs.commander

import javafx.collections.FXCollections
import javafx.scene.control.{TableColumn, TableView}
import javafx.scene.image.Image

import net.diet_rich.dedupfs.commander.fx._

class FilesTable {
  val files = FXCollections.observableArrayList[FilesTableItem]()

  private val iconColumn = new TableColumn[FilesTableItem, Image]("")
  iconColumn.getStyleClass add "iconColumn"
  iconColumn setCellValueFactory callback(_.getValue.icon)
  iconColumn setCellFactory callback(_ => new ImageTableCell[FilesTableItem](17, 17))

  // TODO set column name in css?
  private val nameColumn = new TableColumn[FilesTableItem, String]("Name")
  nameColumn setCellValueFactory callback(_.getValue.name)
  nameColumn setCellFactory ReadWriteSensitiveTableCellFactory()
  nameColumn setOnEditCommit handle(event => event.getRowValue.name.setValue(event.getNewValue))

  // TODO set column name in css?
  private val sizeColumn = new TableColumn[FilesTableItem, String]("Size")
  sizeColumn setCellValueFactory callback(_.getValue.size)

  val table = new TableView[FilesTableItem](files)
  table setEditable true
  table.getColumns.addAll(iconColumn, nameColumn, sizeColumn)
}
