package net.diet_rich.dedupfs.commander

import javafx.scene.control.cell.TextFieldTableCell
import javafx.scene.control.{TableCell, TableColumn}
import javafx.util.Callback
import javafx.util.converter.DefaultStringConverter

object ReadWriteSensitiveTableCellFactory {
  def apply() = new Callback[TableColumn[FilesTableItem, String], TableCell[FilesTableItem, String]] {
    override def call(param: TableColumn[FilesTableItem, String]): TableCell[FilesTableItem, String] = new TextFieldTableCell[FilesTableItem, String](new DefaultStringConverter()) {
      override def updateItem(string: String, isEmpty: Boolean): Unit = {
        super.updateItem(string, isEmpty)
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
