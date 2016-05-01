package net.diet_rich.dedupfs.commander

import java.lang.{Long => JLong}
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
  iconColumn setSortable false

  private val nameColumn = new TableColumn[FilesTableItem, ColumnEntry[String]]("Name")
  // TODO extract method for cell value factory
  nameColumn setCellValueFactory callback(data => new SimpleObjectProperty(ColumnEntry[String](data.getValue.name.getValue, Some(data.getValue.isDirectory))))
  // TODO not generic - just needed here
  nameColumn setCellFactory EditableTableCellFactory(ColumnEntry.stringConverter(identity))
  nameColumn setOnEditCommit handle(event => event.getRowValue.name setValue event.getNewValue.detail)
  nameColumn setComparator ColumnEntry.comparator(nameColumn.getSortType)

  private val sizeColumn = new TableColumn[FilesTableItem, ColumnEntry[JLong]]("Size")
  // TODO extract method for cell value factory
  sizeColumn setCellValueFactory callback(data => new SimpleObjectProperty(ColumnEntry[JLong](data.getValue.size.getValue, Some(data.getValue.isDirectory))))
  sizeColumn setCellFactory sizeCellFactory
  sizeColumn setComparator ColumnEntry.comparator(sizeColumn.getSortType)

  val table = new TableView[FilesTableItem](fileSorted)
  fileSorted.comparatorProperty() bind table.comparatorProperty()
  table setEditable true
  table.getColumns addAll (iconColumn, nameColumn, sizeColumn)
  table.getSortOrder add nameColumn
}

private object FilesTable {
  case class ColumnEntry[T](detail: T, maybeDirectory: Option[Boolean]) {
    def isDirectory = maybeDirectory contains true
  }
  object ColumnEntry {
    def stringConverter[T](convert: String => T) = new StringConverter[ColumnEntry[T]] {
      override def fromString(string: String): ColumnEntry[T] = ColumnEntry[T](convert(string), None)
      override def toString(entry: ColumnEntry[T]): String = entry.detail.toString
    }
    def comparator[T <: Comparable[T]](sortType: => SortType) = new Comparator[ColumnEntry[T]] {
      override def compare(o1: ColumnEntry[T], o2: ColumnEntry[T]): Int = {
        val direction = sortType match {
          case SortType.ASCENDING  =>  1
          case SortType.DESCENDING => -1
        }
        "".compare("")
        (o1.isDirectory, o2.isDirectory) match {
          case (true,  false) => -direction
          case (false, true ) =>  direction
          case (true,  true ) =>  direction * o1.detail.compareTo(o2.detail)
          case (false, false) =>              o1.detail.compareTo(o2.detail)
        }
      }
    }
  }

  case class EditableTableCellFactory[T](stringConverter: StringConverter[T]) extends Callback[TableColumn[FilesTableItem, T], TableCell[FilesTableItem, T]] {
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

  val sizeCellFactory = new Callback[TableColumn[FilesTableItem, ColumnEntry[JLong]], TableCell[FilesTableItem, ColumnEntry[JLong]]] {
    override def call(param: TableColumn[FilesTableItem, ColumnEntry[JLong]]): TableCell[FilesTableItem, ColumnEntry[JLong]] =
      new TextFieldTableCell[FilesTableItem, ColumnEntry[JLong]](ColumnEntry.stringConverter[JLong](_.toLong)) {
        override def updateItem(entry: ColumnEntry[JLong], isEmpty: Boolean): Unit = {
          super.updateItem(entry, isEmpty)
          // TODO number formatting
          if (entry == null) setText("") else setText(entry.detail.toString)
        }
      }
  }
}
