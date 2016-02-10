package net.diet_rich.common

import javafx.application.Platform
import javafx.event.{ActionEvent, EventHandler}
import javafx.scene.image.ImageView
import javafx.scene.input.MouseEvent

package object fx {
  def runFX(f: => Unit) = Platform runLater new Runnable { override def run(): Unit = f }

  def handleAction[T](handler: => T): EventHandler[ActionEvent] = new EventHandler[ActionEvent] {
    override def handle(event: ActionEvent): Unit = handler
  }

  def handleDoubleClick[T](handler: => T): EventHandler[MouseEvent] = new EventHandler[MouseEvent] {
    override def handle(event: MouseEvent): Unit = if (event.getClickCount == 2) handler
  }

  implicit class RichImageView(val view: ImageView) extends AnyVal {
    def fit(width: Double, height: Double): ImageView = init(view) { view =>
      view setFitHeight height
      view setFitWidth width
    }
  }
}
