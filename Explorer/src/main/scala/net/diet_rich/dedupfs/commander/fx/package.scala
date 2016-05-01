package net.diet_rich.dedupfs.commander

import javafx.event.{Event, EventHandler}
import javafx.scene.image.ImageView
import javafx.util.Callback

import net.diet_rich.common._

package object fx {
  def callback[S, T](f: S => T): Callback[S, T] = new Callback[S, T] { override def call(s: S): T = f(s) }
  def handle[T <: Event](f: T => Unit): EventHandler[T] = new EventHandler[T] { override def handle(t: T): Unit = f(t) }

  implicit class RichImageView(val view: ImageView) extends AnyVal {
    def fit(width: Double, height: Double): ImageView = init(view) { view =>
      view setFitHeight height
      view setFitWidth width
    }
  }
}
