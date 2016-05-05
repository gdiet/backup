package net.diet_rich.dedupfs.commander

import java.lang.{Long => JLong}
import java.util.Comparator
import javafx.beans.property.{SimpleObjectProperty, SimpleStringProperty}
import javafx.beans.value.ObservableValue
import javafx.collections.FXCollections
import javafx.collections.transformation.SortedList
import javafx.scene.control.TableColumn.{CellDataFeatures, SortType}
import javafx.scene.control.cell.TextFieldTableCell
import javafx.scene.control._
import javafx.util.{Callback, StringConverter}

import net.diet_rich.common.init
import net.diet_rich.dedupfs.commander.FilesTable._
import net.diet_rich.dedupfs.commander.fx._

// TODO reflect GUI structure in package structure
class FilesTable(cd: FilesTableItem => Unit) {
  val files = FXCollections.observableArrayList[FilesTableItem]()
  private val fileSorted = new SortedList(files)

  private val iconColumn = new TableColumn[FilesTableItem, String]("") // TODO from conf
  iconColumn setCellValueFactory callback { f: CellDataFeatures[FilesTableItem, String] => new SimpleStringProperty(f.getValue.image) }
  iconColumn setCellFactory iconCellFactory
  iconColumn setSortable false

  private val nameColumn = new TableColumn[FilesTableItem, ColumnEntry[String]]("Name") // TODO from conf
  nameColumn setCellValueFactory cellValueFactory(_.name.getValue) // TODO why unpack and re-wrap the name?
  nameColumn setCellFactory nameCellFactory
  nameColumn setOnEditCommit handle(event => event.getRowValue.name setValue event.getNewValue.detail)
  nameColumn setComparator ColumnEntry.columnComparator(nameColumn.getSortType, _ compareToIgnoreCase _)

  private val sizeColumn = new TableColumn[FilesTableItem, ColumnEntry[JLong]]("Size") // TODO from conf
  sizeColumn setCellValueFactory cellValueFactory(_.size.getValue) // TODO why unpack and re-wrap the size?
  sizeColumn setCellFactory sizeCellFactory
  sizeColumn setComparator ColumnEntry.columnComparator(sizeColumn.getSortType, _ compareTo _)

  val table = new TableView[FilesTableItem](fileSorted)
  fileSorted.comparatorProperty() bind table.comparatorProperty()
  table setEditable true
  table.getColumns addAll (iconColumn, nameColumn, sizeColumn)
  table.getSortOrder add nameColumn

  table.setRowFactory(new Callback[TableView[FilesTableItem], TableRow[FilesTableItem]] {
    override def call(param: TableView[FilesTableItem]): TableRow[FilesTableItem] = init(new TableRow[FilesTableItem]) { row =>
      row setOnMouseClicked handle { event =>
        if (event.getClickCount == 2) {
          if (row.getItem.isDirectory) cd(row.getItem) else row.getItem open()
        }
        event consume()
      }
    }
  })

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
    def columnComparator[T](sortType: => SortType, comparator: (T, T) => Int) = new Comparator[ColumnEntry[T]] {
      override def compare(o1: ColumnEntry[T], o2: ColumnEntry[T]): Int = {
        val direction = sortType match {
          case SortType.ASCENDING  =>  1
          case SortType.DESCENDING => -1
        }
        "".compare("")
        (o1.isDirectory, o2.isDirectory) match {
          case (true,  false) => -direction
          case (false, true ) =>  direction
          case (true,  true ) =>  direction * comparator(o1.detail, o2.detail)
          case (false, false) =>              comparator(o1.detail, o2.detail)
        }
      }
    }
  }

  def cellValueFactory[T](extractor: FilesTableItem => T) = callback[CellDataFeatures[FilesTableItem, ColumnEntry[T]], ObservableValue[ColumnEntry[T]]] {
    data: CellDataFeatures[FilesTableItem, ColumnEntry[T]] =>
      new SimpleObjectProperty(ColumnEntry[T](extractor(data.getValue), Some(data.getValue.isDirectory)))
  }

  val iconCellFactory = callback[TableColumn[FilesTableItem, String], TableCell[FilesTableItem, String]] {
    new TableCell[FilesTableItem, String] {
      override def updateItem(key: String, empty: Boolean): Unit = {
        if (!empty) setGraphic(imageView(key))
      }
    }
  }

  val nameCellFactory = callback[TableColumn[FilesTableItem, ColumnEntry[String]], TableCell[FilesTableItem, ColumnEntry[String]]] {
    new TextFieldTableCell[FilesTableItem, ColumnEntry[String]](ColumnEntry.stringConverter(identity)) {
      override def updateItem(entry: ColumnEntry[String], isEmpty: Boolean): Unit = {
        super.updateItem(entry, isEmpty)
        setEditable(
          (getTableRow != null) && {
            val rowIndex = getTableRow.getIndex
            val items = getTableView.getItems
            rowIndex >= 0 && rowIndex < items.size() && items.get(rowIndex).isEditable
          }
        )
      }
    }
  }

  val sizeCellFactory = callback[TableColumn[FilesTableItem, ColumnEntry[JLong]], TableCell[FilesTableItem, ColumnEntry[JLong]]] {
    // TODO align right
    new TextFieldTableCell[FilesTableItem, ColumnEntry[JLong]](ColumnEntry.stringConverter(_.toLong)) {
      override def updateItem(entry: ColumnEntry[JLong], isEmpty: Boolean): Unit = {
        super.updateItem(entry, isEmpty)
        // TODO number formatting from conf
        if (entry == null || entry.isDirectory) setText("") else setText(entry.detail.toString)
      }
    }
  }
}
