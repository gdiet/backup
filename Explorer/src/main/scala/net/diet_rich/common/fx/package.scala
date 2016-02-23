package net.diet_rich.common

import javafx.application.Platform
import javafx.event.{Event, ActionEvent, EventHandler}
import javafx.scene.Node
import javafx.scene.image.ImageView
import javafx.scene.input.MouseEvent

package object fx {
  def runFX(f: => Unit) = Platform runLater new Runnable { override def run(): Unit = f }

  def handle[T <: Event](handler: T => Any): EventHandler[T] = new EventHandler[T] {
    override def handle(event: T): Unit = handler(event)
  }
  def handleAction(handler: => Any): EventHandler[ActionEvent] = handle(_ => handler)
  def handleDoubleClick(handler: => Any): EventHandler[MouseEvent] = handle(e => if(e.getClickCount == 2) handler)

  implicit class RichNode[T <: Node](val node: T) extends AnyVal {
    def styled(styleClass: String): T = init(node) { _.getStyleClass.add(styleClass) }
  }

  implicit class RichImageView(val view: ImageView) extends AnyVal {
    def fit(width: Double, height: Double): ImageView = init(view) { view =>
      view setFitHeight height
      view setFitWidth width
    }
  }
}
