package net.diet_rich.dedupfs.explorer

import java.io.File
import javafx.application.Application
import javafx.scene.Scene
import javafx.stage.Stage

import net.diet_rich.dedupfs.explorer.driver.dedup.DedupFiles
import net.diet_rich.dedupfs.explorer.driver.physical.PhysicalFiles
import net.diet_rich.dedupfs.explorer.filesPane.FileSystemRegistry
import net.diet_rich.dedupfs.{Repository, StoreMethod}

object Main extends App {
  Application launch(classOf[Main], args: _*)
}

class Main extends Application {
  override def start(stage: Stage): Unit = {
    val writable = false
    val storeMethod = StoreMethod.STORE
    val directory = new File("../test")
    val repository = if (writable) Repository openReadWrite(directory, storeMethod) else Repository openReadOnly directory
    sys addShutdownHook repository.close()

    val registry = new FileSystemRegistry()
    PhysicalFiles registerIn registry
    DedupFiles registerIn(repository, registry)
    val explorer = new DoubleExplorerPane(registry, "file://../test/_files", "dup:///")

    val scene = new Scene(explorer.component)
    scene.getStylesheets add "explorer_style.css"
    explorer registerIn scene
    stage setScene scene
    stage show()
  }
}
