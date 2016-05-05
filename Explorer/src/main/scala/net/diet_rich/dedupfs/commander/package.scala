package net.diet_rich.dedupfs

import javafx.scene.image.{Image, ImageView}

import com.typesafe.config.ConfigFactory
import net.diet_rich.common.init

package object commander {
  val conf = ConfigFactory.load()

  val imageFile   = new Image(conf getString "icons.file")
  val imageFolder = new Image(conf getString "icons.folder")
  val imageReload = new Image(conf getString "icons.reload")
  val imageUp     = new Image(conf getString "icons.up")

  def imageView(key: String): ImageView = init(new ImageView(new Image(conf getString s"$key.icon"))) { imageView =>
    imageView.setFitWidth (conf getDoubleList s"$key.size" get 0)
    imageView.setFitHeight(conf getDoubleList s"$key.size" get 1)
  }
}
