package net.diet_rich.dedupfs

import javafx.beans.value.{ObservableValueBase, ObservableValue}
import javafx.scene.control.{TableView, TableCell, TableColumn}
import javafx.scene.control.TableColumn.CellDataFeatures
import javafx.scene.image.Image
import javafx.util.Callback

import net.diet_rich.common.init

package object explorer {
  val imageFile = new Image("com.modernuiicons/appbar.page.png")
  val imageFolder = new Image("com.modernuiicons/appbar.folder.png")
  val imageReload = new Image("com.modernuiicons/appbar.refresh.png")
  val imageUp = new Image("com.modernuiicons/appbar.chevron.up.png")

  def createTableColumn[T](title: String, cellRendering: (TableCell[T, T], T) => Unit) =
    new TableColumn[T, T](title).withFilledCells(cellRendering)

  implicit class RichTableView[T](val table: TableView[T]) extends AnyVal {
    def withColumn(title: String, cellRendering: (TableCell[T, T], T) => Unit) = init(table) { table =>
      table.getColumns add createTableColumn(title, cellRendering)
    }
    def withColumn(column: TableColumn[T, T]) = init(table) { table =>
      table.getColumns add column
    }
  }

  implicit class RichTableColumn[T](val column: TableColumn[T, T]) extends AnyVal {
    def withFilledCells(cellRendering: (TableCell[T, T], T) => Unit) = init(column) { column =>
      column setCellValueFactory new Callback[CellDataFeatures[T, T], ObservableValue[T]] {
        def call(p: CellDataFeatures[T, T]): ObservableValue[T] = {
          new ObservableValueBase[T] {
            override def getValue: T = p.getValue
          }
        }
      }
      column setCellFactory new Callback[TableColumn[T, T], TableCell[T, T]] {
        override def call(param: TableColumn[T, T]): TableCell[T, T] = new TableCell[T, T]() {
          override def updateItem(item: T, empty: Boolean): Unit = {
            if (!empty) cellRendering(this, item)
          }
        }
      }
    }
  }
}
