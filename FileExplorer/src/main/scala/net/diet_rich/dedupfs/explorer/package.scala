package net.diet_rich.dedupfs

import javafx.scene.image.{Image, ImageView}

import com.typesafe.config.ConfigFactory
import net.diet_rich.common.init

package object explorer {
  val conf = ConfigFactory.load()

  private def image(url: String): Image = try new Image(url) catch {
    case e: IllegalArgumentException => throw new IllegalArgumentException(s"Invalid image URL: $url", e)
  }
  def imageView(key: String): ImageView = init(new ImageView(image(conf getString s"$key.icon"))) { imageView =>
    imageView.setFitWidth (conf getDoubleList s"$key.size" get 0)
    imageView.setFitHeight(conf getDoubleList s"$key.size" get 1)
  }
}
