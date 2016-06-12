package net.diet_rich.dedupfs.explorer

import java.util.function.Consumer
import javafx.scene.control.{Button, SplitPane}
import javafx.scene.input.{KeyCode, KeyEvent}
import javafx.scene.layout.{BorderPane, HBox, Priority}
import javafx.scene.{Node, Parent, Scene}

import net.diet_rich.common._
import net.diet_rich.common.fx._
import net.diet_rich.dedupfs.explorer.filesPane.{Continue, FileSystemRegistry, FilesPane}

class DoubleExplorerPane(registry: FileSystemRegistry, initialUrlLeft: String, initialUrlRight: String) {
  private val leftPane = new FilesPane(registry, initialUrlLeft)
  private val rightPane = new FilesPane(registry, initialUrlRight)
  private val splitPane = new SplitPane(leftPane.component, rightPane.component)

  private var activePane = init(leftPane) {_ setActive true}
  private def otherPane = if (activePane == leftPane) rightPane else leftPane
  private val sceneFocusChangeListener = changeListener { node: Node =>
    if (nodesUp(node) contains otherPane.component) {
      activePane setActive false
      otherPane setActive true
      activePane = otherPane
    }
  }
  def registerIn(scene: Scene): Unit = scene.focusOwnerProperty addListener sceneFocusChangeListener

  private val keyEventFilter = handle { e: KeyEvent =>
    (e.getEventType, e.getCode, e.getText) match {
      case (KeyEvent.KEY_RELEASED, KeyCode.F2, _) => renameAction(); e.consume()
      case (KeyEvent.KEY_RELEASED, KeyCode.F5, _) => copyAction(); e.consume()
      case (KeyEvent.KEY_RELEASED, KeyCode.F6, _) => moveAction(); e.consume()
      case _ => ()
    }
  }

  private def renameAction(): Unit = activePane renameSingleSelection()
  private def copyAction(): Unit = otherPane.directory foreach { other =>
    runAsync(other copyHere(activePane.selectedFiles, (_, _) => Continue))
  }
  private def moveAction(): Unit = otherPane.directory foreach { other =>
    runAsync(other moveHere(activePane.selectedFiles, (_, _) => Continue))
  }

  val component: Parent = init(new BorderPane()) { pane =>
    pane addEventFilter(KeyEvent.KEY_RELEASED, keyEventFilter)
    pane setCenter splitPane
    pane setBottom init(new HBox()) { bottom =>
      bottom.getChildren add init(new Button(conf getString "button.rename.label")) { renameButton =>
        renameButton setOnAction handle(renameAction())
      }
      bottom.getChildren add init(new Button(conf getString "button.copy.label")) { copyButton =>
        copyButton setOnAction handle(copyAction())
      }
      bottom.getChildren add init(new Button(conf getString "button.move.label")) { moveButton =>
        moveButton setOnAction handle(moveAction())
      }
      bottom.getChildren forEach new Consumer[Node] {
        override def accept(node: Node): Unit = node match {
          case button: Button =>
            HBox setHgrow(button, Priority.ALWAYS)
            button setPrefWidth 40
            button setMaxWidth Double.MaxValue
        }
      }
    }
  }
}
