package net.diet_rich.dedupfs

import java.util.Comparator
import javafx.beans.value.{ObservableValueBase, ObservableValue}
import javafx.scene.control.{TableView, TableCell, TableColumn}
import javafx.scene.control.TableColumn.{SortType, CellDataFeatures}
import javafx.scene.image.Image
import javafx.util.Callback

import net.diet_rich.common.init
import net.diet_rich.dedupfs.explorer.ExplorerFile.AFile

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

  implicit class RichFileTableColumn(val column: TableColumn[AFile, AFile]) extends AnyVal {
    def withFileComparator(comparator: (AFile, AFile) => Int) = init(column) { column =>
      column.setComparator(new Comparator[AFile] {
        override def compare(o1: AFile, o2: AFile): Int = {
          val direction = column.getSortType match {
            case SortType.ASCENDING  =>  1
            case SortType.DESCENDING => -1
          }
          (o1.isDirectory, o2.isDirectory) match {
            case (true,  false) => -direction
            case (false, true ) =>  direction
            case (true,  true ) =>  direction * o1.name.compare(o2.name)
            case (false, false) =>  comparator(o1, o2)
          }
        }
      })
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
