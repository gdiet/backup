package net.diet_rich.dedupfs.commander

import java.io.File
import java.lang.Thread.UncaughtExceptionHandler
import javafx.application.Application
import javafx.beans.property.{LongProperty, SimpleLongProperty}
import javafx.scene.Scene
import javafx.stage.Stage

import net.diet_rich.dedupfs.{DedupFile, FileSystem, Repository, StoreMethod}
import net.diet_rich.dedupfs.commander.NameContainer.VetoException

object Main extends App {
  Application launch (classOf[Main], args:_*)
}

// TODO clean up
case class DedupFilesTableItem(file: DedupFile) extends FilesTableItem {
  override def name: NameContainer = NameContainer(file.name) // TODO vetoer
  override def isEditable: Boolean = file.isWritable
  override def asFilesPaneItem: Option[FilesPaneDirectory] = Some(DedupFilePaneDirectory(file)) // TODO check if not exists or nor dir
  override def size: LongProperty = new SimpleLongProperty(file.size)
  override def time: LongProperty = new SimpleLongProperty(file.lastModified)
  override def isDirectory: Boolean = file.isDirectory
  override def open(): Unit = ???
  override def image: String = if (file.isDirectory) "image.folder" else "image.file"
}

case class DedupFilePaneDirectory(file: DedupFile) extends FilesPaneDirectory {
  override def list: Seq[FilesTableItem] = file.children.toSeq map DedupFilesTableItem
  override def url: String = s"dfs://${file.path}"
  override def up: FilesPaneDirectory = DedupFilePaneDirectory(file.parent)
}

class Main extends Application {
  override def init(): Unit = {}
  override def stop(): Unit = {}

  override def start(stage: Stage): Unit = {
    val writable = false
    val storeMethod = StoreMethod.STORE
    val directory = new File("test")
    val repository = if (writable) Repository openReadWrite(directory, storeMethod) else Repository openReadOnly directory
    val fileSystem = new FileSystem(repository)
    sys addShutdownHook fileSystem.close()

    FileSystemRegistry add ("dfs", path =>
      // TODO check if not exists
      Some(DedupFilePaneDirectory(fileSystem.getFile(path)))
    )

    val leftRoot = "dfs:///"
    val rightRoot = """file://e:\georg\zeugs\2.4.0.03\iTBClient-win\bin"""
    val view = new MainPane(leftRoot, rightRoot)
    stage.setScene(net.diet_rich.common.init(new Scene(view.component)) { scene =>
      scene.getStylesheets add "commander_style.css"
      scene.focusOwnerProperty addListener view.sceneFocusChangeListener
    })
    stage.show()

    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = e match {
        case e: VetoException => ()
        case _ => e.printStackTrace()
      }
    })
  }
}
