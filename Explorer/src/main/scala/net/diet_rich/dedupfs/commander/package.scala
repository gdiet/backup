package net.diet_rich.dedupfs

import javafx.scene.image.{Image, ImageView}

import com.typesafe.config.ConfigFactory
import net.diet_rich.common.init

package object commander {
  val conf = ConfigFactory.load()

  def imageView(key: String): ImageView = init(new ImageView(new Image(conf getString s"$key.icon"))) { imageView =>
    imageView.setFitWidth (conf getDoubleList s"$key.size" get 0)
    imageView.setFitHeight(conf getDoubleList s"$key.size" get 1)
  }
}
