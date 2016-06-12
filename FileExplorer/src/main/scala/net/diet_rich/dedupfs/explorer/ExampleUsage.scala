package net.diet_rich.dedupfs.explorer

import javafx.application.Application
import javafx.scene.Scene
import javafx.stage.Stage

import net.diet_rich.dedupfs.explorer.driver.physical.PhysicalFiles
import net.diet_rich.dedupfs.explorer.filesPane.FileSystemRegistry

object ExampleUsage extends App {
  Application launch(classOf[ExampleUsage], args: _*)
}

class ExampleUsage extends Application {
  override def start(stage: Stage): Unit = {
    val registry = PhysicalFiles registerIn new FileSystemRegistry()
    val explorer = new DoubleExplorerPane(registry, "file://../test/_files", "file://../test/_files")
    val scene = new Scene(explorer.component)
    scene.getStylesheets add "explorer_style.css"
    explorer registerIn scene
    stage setScene scene
    stage show()
  }
}
