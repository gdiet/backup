package net.diet_rich.dedupfs.commander

import java.util.Comparator
import javafx.beans.property.SimpleObjectProperty
import javafx.collections.FXCollections
import javafx.collections.transformation.SortedList
import javafx.scene.control.TableColumn.SortType
import javafx.scene.control.cell.TextFieldTableCell
import javafx.scene.control.{TableCell, TableColumn, TableView}
import javafx.scene.image.Image
import javafx.util.{Callback, StringConverter}

import net.diet_rich.dedupfs.commander.FilesTable._
import net.diet_rich.dedupfs.commander.fx._

class FilesTable {
  val files = FXCollections.observableArrayList[FilesTableItem]()
  private val fileSorted = new SortedList(files)

  private val iconColumn = new TableColumn[FilesTableItem, Image]("")
  iconColumn.getStyleClass add "iconColumn"
  iconColumn setCellValueFactory callback(_.getValue.icon)
  iconColumn setCellFactory callback(new ImageTableCell[FilesTableItem](17, 17))

  private val nameColumn = new TableColumn[FilesTableItem, NameColumnEntry]("Name")
  nameColumn setCellValueFactory callback(data => new SimpleObjectProperty(NameColumnEntry(data.getValue)))
  nameColumn setCellFactory EditableTableCellFactory(NameColumnEntry.stringConverter)
  nameColumn setOnEditCommit handle(event => event.getRowValue.name setValue event.getNewValue.name)
  nameColumn setComparator NameColumnEntry.comparator(nameColumn.getSortType)

  private val sizeColumn = new TableColumn[FilesTableItem, String]("Size")
  sizeColumn setCellValueFactory callback(_.getValue.size)

  val table = new TableView[FilesTableItem](fileSorted)
  fileSorted.comparatorProperty() bind table.comparatorProperty()
  table setEditable true
  table.getColumns addAll (iconColumn, nameColumn, sizeColumn)
  table.getSortOrder add nameColumn
}

object FilesTable {
  private case class NameColumnEntry(name: String, maybeDirectory: Option[Boolean]) {
    def isDirectory = maybeDirectory contains true
  }
  private object NameColumnEntry {
    def apply(file: FilesTableItem): NameColumnEntry = NameColumnEntry(file.name.getValue, Some(file.isDirectory))
    val stringConverter = new StringConverter[NameColumnEntry] {
      override def fromString(string: String): NameColumnEntry = NameColumnEntry(string, None)
      override def toString(entry: NameColumnEntry): String = entry.name
    }
    def comparator(sortType: => SortType) = new Comparator[NameColumnEntry] {
      override def compare(o1: NameColumnEntry, o2: NameColumnEntry): Int = {
        val direction = sortType match {
          case SortType.ASCENDING  =>  1
          case SortType.DESCENDING => -1
        }
        (o1.isDirectory, o2.isDirectory) match {
          case (true,  false) => -direction
          case (false, true ) =>  direction
          case (true,  true ) =>  direction * o1.name.compare(o2.name)
          case (false, false) =>              o1.name.compare(o2.name)
        }
      }
    }
  }

  private object EditableTableCellFactory {
    def apply[T](stringConverter: StringConverter[T]) = new Callback[TableColumn[FilesTableItem, T], TableCell[FilesTableItem, T]] {
      override def call(param: TableColumn[FilesTableItem, T]): TableCell[FilesTableItem, T] = new TextFieldTableCell[FilesTableItem, T](stringConverter) {
        override def updateItem(entry: T, isEmpty: Boolean): Unit = {
          super.updateItem(entry, isEmpty)
          setEditable(
            if (getTableRow != null) {
              val rowIndex = getTableRow.getIndex
              val items = getTableView.getItems
              rowIndex >= 0 && rowIndex < items.size() && items.get(rowIndex).isEditable
            } else false
          )
        }
      }
    }
  }
}
