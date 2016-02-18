package net.diet_rich.dedupfs.explorer

import java.io.File
import javafx.application.Application
import javafx.scene.Scene
import javafx.stage.Stage

import scala.collection.JavaConverters._

import net.diet_rich.common.Arguments
import net.diet_rich.dedupfs.{FileSystem, Repository, StoreMethod}

object Main extends App {
  Application launch (classOf[Main], args:_*)
}

class Main() extends Application {
  private var fileSystem: FileSystem = null
  override def init(): Unit = {
    val args = getParameters.getRaw.asScala.toArray
    val arguments = new Arguments(args, 1)
    val List(repoPath) = arguments.parameters
    val writable = arguments booleanOptional "writable" getOrElse false
    val storeMethod = arguments optional "storeMethod" map StoreMethod.named getOrElse StoreMethod.STORE
    arguments withSettingsChecked {
      val directory = new File(repoPath)
      val repository = if (writable) Repository openReadWrite(directory, storeMethod) else Repository openReadOnly directory
      fileSystem = new FileSystem(repository)
    }
    require(fileSystem != null, "Could not initialize file system")
  }
  override def stop(): Unit = fileSystem close()
  override def start(stage: Stage): Unit = {
    val parent = new Commander(FileSystems, DedupSystemFile(fileSystem.getFile("")))
    stage setScene net.diet_rich.common.init(new Scene(parent.component)) { _.getStylesheets.add("style.css") }
    stage.show()
  }
}
