package net.diet_rich.dedupfs.commander

import java.io.File
import java.lang.Thread.UncaughtExceptionHandler
import javafx.application.Application
import javafx.scene.Scene
import javafx.stage.Stage

import net.diet_rich.dedupfs.commander.NameContainer.VetoException

object Main extends App {
  Application launch (classOf[Main], args:_*)
}

class Main extends Application {
  override def init(): Unit = {} // TODO
  override def stop(): Unit = {} // TODO

  override def start(stage: Stage): Unit = {
    val directory = PhysicalFilesPaneItem(new File("""e:\georg\zeugs\2.4.0.03\iTBClient-win\bin"""))
    val filesPane = new FilesPane(directory)

    stage.setScene(net.diet_rich.common.init(new Scene(filesPane.component)) { _.getStylesheets.add("commander_style.css") })
    stage.show()

    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = e match {
        case e: VetoException => ()
        case _ => e.printStackTrace()
      }
    })
  }
}