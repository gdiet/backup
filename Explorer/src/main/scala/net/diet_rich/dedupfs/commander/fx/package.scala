package net.diet_rich.dedupfs.commander

import javafx.application.Platform
import javafx.beans.value.{ChangeListener, ObservableValue}
import javafx.css.Styleable
import javafx.event.{Event, EventHandler}
import javafx.scene.Node
import javafx.scene.image.ImageView
import javafx.util.Callback

import net.diet_rich.common._

package object fx {
  def runFX(f: => Unit) = Platform runLater new Runnable { override def run(): Unit = f }

  def callback[S, T](f: S => T): Callback[S, T] = new Callback[S, T] { override def call(s: S): T = f(s) }
  def callback[S, T](f:   => T): Callback[S, T] = new Callback[S, T] { override def call(s: S): T = f }
  def handle[T <: Event](f: T => Unit): EventHandler[T] = new EventHandler[T] { override def handle(t: T): Unit = f(t) }
  def handle[T <: Event](f:   => Unit): EventHandler[T] = new EventHandler[T] { override def handle(t: T): Unit = f }
  def changeListener[T](f: T => Unit): ChangeListener[T] = new ChangeListener[T] { override def changed(observable: ObservableValue[_ <: T], oldValue: T, newValue: T): Unit = f(newValue) }

  /** @return the parameter node and its parents */
  def nodesUp(node: Node): Stream[Node] = node #:: (if (node.getParent == null) Stream.Empty else nodesUp(node.getParent))

  implicit class RichStyleable[T <: Styleable](val item: T) extends AnyVal {
    def withStyle(style: String): T = init(item) { _.getStyleClass add style }
    def withoutStyle(style: String): T = init(item) { _.getStyleClass remove style }
  }

  implicit class RichImageView(val view: ImageView) extends AnyVal {
    def fit(width: Double, height: Double): ImageView = init(view) { view => view setFitHeight height; view setFitWidth width }
  }
}
