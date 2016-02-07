package net.diet_rich.dedupfs

import javafx.scene.image.{ImageView, Image}

package object explorer {
  val imageFile = new Image("com.modernuiicons/appbar.page.png")
  val imageFolder = new Image("com.modernuiicons/appbar.folder.png")
  val imageReload = new Image("com.modernuiicons/appbar.refresh.png")
  val imageUp = new Image("com.modernuiicons/appbar.chevron.up.png")

  implicit class RichImageView(val view: ImageView) extends AnyVal {
    def fit(width: Double, height: Double): ImageView = {
      view setFitHeight height
      view setFitWidth width
      view
    }
  }
}
