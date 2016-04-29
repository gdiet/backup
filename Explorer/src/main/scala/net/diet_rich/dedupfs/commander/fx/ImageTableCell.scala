package net.diet_rich.dedupfs.commander.fx

import javafx.scene.control.TableCell
import javafx.scene.image.{Image, ImageView}

class ImageTableCell[T](width: Double, height: Double) extends TableCell[T, Image] {
  // getStyleClass.add("imageTableCell") // TODO size with css, not in code
  override def updateItem(image: Image, empty: Boolean): Unit = {
    if (!empty) setGraphic(new ImageView(image).fit(width, height))
  }
}
