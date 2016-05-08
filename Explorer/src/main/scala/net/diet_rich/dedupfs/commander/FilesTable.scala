package net.diet_rich.dedupfs.commander

import java.text.SimpleDateFormat
import java.util.{Comparator, Date, Locale}
import javafx.beans.value.{ObservableValue, ObservableValueBase}
import javafx.collections.FXCollections
import javafx.collections.transformation.SortedList
import javafx.scene.control.TableColumn.{CellDataFeatures, SortType}
import javafx.scene.control._
import javafx.scene.control.cell.TextFieldTableCell
import javafx.scene.input.KeyCode
import javafx.util.{Callback, StringConverter}

import net.diet_rich.common.init
import net.diet_rich.dedupfs.commander.FilesTable._
import net.diet_rich.dedupfs.commander.fx._

class FilesTable(cd: FilesTableItem => Unit) {
  val files = FXCollections.observableArrayList[FilesTableItem]()
  private val fileSorted = new SortedList(files)

  private val iconColumn = new TableColumn[FilesTableItem, FilesTableItem](conf getString "column.icon.label")
  iconColumn setCellValueFactory cellValueFactory(identity)
  iconColumn setCellFactory cellFactory { case (entry, cell) => cell setGraphic imageView(entry.image) }
  iconColumn setSortable false

  private val nameColumn = new TableColumn[FilesTableItem, NameEntry](conf getString "column.name.label")
  nameColumn setCellValueFactory cellValueFactory(file => NameEntry(file.name.getValue, Some(file.isDirectory)), Some(_.name))
  nameColumn setCellFactory nameCellFactory
  nameColumn setOnEditCommit handle { event => event.getRowValue.name setValue event.getNewValue.detail; table setEditable false }
  nameColumn setOnEditCancel handle { table setEditable false }
  nameColumn setComparator columnComparator(nameColumn, _.isDirectory, _.detail compareToIgnoreCase _.detail)

  private val sizeColumn = new TableColumn[FilesTableItem, FilesTableItem](conf getString "column.size.label")
  sizeColumn withStyle "sizeColumn"
  sizeColumn setCellValueFactory cellValueFactory(identity, Some(_.size))
  sizeColumn setCellFactory cellFactory { case (entry, cell) =>
    if (entry.isDirectory) cell setText "" else cell setText (
      conf getString "column.size.numberFormat" formatLocal (Locale forLanguageTag (conf getString "column.size.numberLocale"), entry.size.longValue)
    )
  }
  sizeColumn setComparator columnComparator(sizeColumn, _.isDirectory, _.size.longValue compare _.size.longValue)

  private val dateColumn = new TableColumn[FilesTableItem, FilesTableItem](conf getString "column.date.label")
  dateColumn setCellValueFactory cellValueFactory(identity, Some(_.time))
  dateColumn setCellFactory cellFactory { case (entry, cell) => cell setText new SimpleDateFormat(conf getString "column.date.dateFormat", Locale forLanguageTag (conf getString "column.date.formatLocale")).format(new Date(entry.time.longValue)) }
  dateColumn setComparator columnComparator(dateColumn, _.isDirectory, _.time.longValue compare _.time.longValue)

  val table = new TableView[FilesTableItem](fileSorted)
  fileSorted.comparatorProperty bind table.comparatorProperty
  table.getColumns addAll (iconColumn, nameColumn, sizeColumn, dateColumn)
  table.getSortOrder add nameColumn
  // enable table editing only on F2 release, and disable it on edit commit/cancel (see there)
  table setOnKeyReleased handle { _.getCode match {
    case KeyCode.F2 =>
      table setEditable true
      table edit (table.getFocusModel.getFocusedCell.getRow, nameColumn)
    case _ => ()
  }}

  table.setRowFactory(new Callback[TableView[FilesTableItem], TableRow[FilesTableItem]] {
    override def call(param: TableView[FilesTableItem]): TableRow[FilesTableItem] = init(new TableRow[FilesTableItem]) { row =>
      row setOnMouseClicked handle { event =>
        if (event.getClickCount == 2 && row.getItem != null) {
          if (row.getItem.isDirectory) cd(row.getItem) else row.getItem open()
        }
        event consume()
      }
    }
  })
}

private object FilesTable {
  def cellValueFactory[T](f: FilesTableItem => T, bindTo: Option[FilesTableItem => ObservableValue[_]] = None) =
    callback[CellDataFeatures[FilesTableItem, T], ObservableValue[T]] { data: CellDataFeatures[FilesTableItem, T] =>
      new ObservableValueBase[T] {
        val getValue: T = f(data.getValue)
        bindTo foreach {_(data.getValue) addListener changeListener {_: Any => fireValueChangedEvent()}}
      }
    }

  def cellFactory(f: (FilesTableItem, TableCell[FilesTableItem, FilesTableItem]) => Unit) = new Callback[TableColumn[FilesTableItem, FilesTableItem], TableCell[FilesTableItem, FilesTableItem]] {
    override def call(param: TableColumn[FilesTableItem, FilesTableItem]): TableCell[FilesTableItem, FilesTableItem] =
      new TableCell[FilesTableItem, FilesTableItem] {
        override def updateItem(entry: FilesTableItem, isEmpty: Boolean): Unit = {
          super.updateItem(entry, isEmpty)
          if (!isEmpty) f(entry, this)
        }
      }
  }

  def columnComparator[T](column: TableColumn[FilesTableItem, T], isDirectory: T => Boolean, comparator: (T, T) => Int) = new Comparator[T] {
    override def compare(file1: T, file2: T): Int = {
      val direction = column.getSortType match {
        case SortType.ASCENDING  =>  1
        case SortType.DESCENDING => -1
      }
      (isDirectory(file1), isDirectory(file2)) match {
        case (true,  false) => -direction
        case (false, true ) =>  direction
        case (true,  true ) =>  direction * comparator(file1, file2)
        case (false, false) =>              comparator(file1, file2)
      }
    }
  }

  case class NameEntry(detail: String, maybeDirectory: Option[Boolean]) {
    def isDirectory = maybeDirectory contains true
  }

  val nameEntryStringConverter = new StringConverter[NameEntry] {
    override def fromString(string: String): NameEntry = NameEntry(string, None)
    override def toString(entry: NameEntry): String = entry.detail.toString
  }

  val nameCellFactory = callback[TableColumn[FilesTableItem, NameEntry], TableCell[FilesTableItem, NameEntry]] {
    new TextFieldTableCell[FilesTableItem, NameEntry](nameEntryStringConverter) {
      override def updateItem(entry: NameEntry, isEmpty: Boolean): Unit = {
        super.updateItem(entry, isEmpty)
        setEditable(
          (getTableRow != null) && {
            val rowIndex = getTableRow.getIndex
            val items = getTableView.getItems
            rowIndex >= 0 && rowIndex < items.size && items.get(rowIndex).isEditable
          }
        )
      }
    }
  }
}
