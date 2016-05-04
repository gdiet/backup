package net.diet_rich.dedupfs.commander.fx

import javafx.scene.control.TableCell
import javafx.scene.image.{Image, ImageView}

class ImageTableCell[T](width: Double, height: Double) extends TableCell[T, Image] {
  override def updateItem(image: Image, empty: Boolean): Unit = {
    // Note: The size of an ImageView can't be set using CSS, see https://docs.oracle.com/javase/8/javafx/api/javafx/scene/doc-files/cssref.html#node
    if (!empty) setGraphic(new ImageView(image).fit(width, height))
  }
}
