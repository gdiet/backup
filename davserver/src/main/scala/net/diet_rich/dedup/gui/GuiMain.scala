// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.gui

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.language.implicitConversions
import javafx.application.Application
import javafx.application.Platform
import javafx.event.Event
import javafx.event.EventHandler
import javafx.fxml.FXMLLoader
import javafx.scene.Parent
import javafx.scene.Scene
import javafx.scene.control.Button
import javafx.stage.DirectoryChooser
import javafx.stage.Stage
import java.io.File
import javafx.scene.control.TextField
import javafx.stage.StageStyle
import javafx.stage.WindowEvent

object GuiMain extends App {
  Application launch classOf[GuiMain]
  
  import language.implicitConversions
  implicit def functionAsEventHandler[E <: Event](f: => Unit): EventHandler[E] =
    new EventHandler[E]() { override def handle(e: E) = f }

  implicit def functionWithBracesAsEventHandler[E <: Event](f: () => Unit): EventHandler[E] =
    new EventHandler[E]() { override def handle(e: E) = f() }
  
  implicit def functionAsEventHandler[E <: Event](f: E => Unit): EventHandler[E] =
    new EventHandler[E]() { override def handle(e: E) = f(e) }
  
  def inFXApplicationThread(f: => Unit): Unit =
    Platform.runLater(new Runnable() { override def run = f })
}
class GuiMain extends Application { import GuiMain._
  override def start(stage: Stage): Unit = {
    val fxml = getClass getResource "/servergui.fxml"
    val root: Parent = FXMLLoader load fxml
    val guiMap = root.getChildrenUnmodifiable.asScala map {c => c.idProperty().get() -> c} toMap
    val scene = new Scene(root)

    val repoField:   TextField = guiMap("repositoryTextField").asInstanceOf[TextField]
    val repoChooser: Button    = guiMap("repositoryChooserButton").asInstanceOf[Button]
    val startButton: Button    = guiMap("startServerButton").asInstanceOf[Button]
    val exitButton:  Button    = guiMap("exitButton").asInstanceOf[Button]

    repoChooser setOnAction {
      // immediately load repository
      chooseDir(stage) foreach { dir => repoField setText dir.getPath }
    }
    // TODO disable start server if no repository chosen
    startButton setOnAction { () =>
      repoChooser setDisable true
      startButton setDisable true
      repoField setDisable true
      println("I should start the server now ...")
    }
    exitButton setOnAction println("I should exit now ...")
    
    stage setTitle "Deduplication Backup"
    stage setScene scene
    stage setOnCloseRequest { e: WindowEvent => println("I should exit now, too ..."); e.consume }
    stage show
  }

  def chooseDir(stage: Stage): Option[File] = {
    val chooser = new DirectoryChooser()
    chooser setTitle "select repository directory"
    Option(chooser showDialog(stage))
  }
}
