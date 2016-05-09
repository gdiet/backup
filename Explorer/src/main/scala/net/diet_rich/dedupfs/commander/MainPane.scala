package net.diet_rich.dedupfs.commander

import java.util.function.Consumer
import javafx.scene.control.{Button, SplitPane}
import javafx.scene.layout.{BorderPane, HBox, Priority}
import javafx.scene.{Node, Parent}

import net.diet_rich.common._
import net.diet_rich.dedupfs.commander.fx._

class MainPane(initialUrlLeft: String, initialUrlRight: String) {
  private val leftPane = new FilesPane(initialUrlLeft)
  private val rightPane = new FilesPane(initialUrlRight)
  private val splitPane = new SplitPane(leftPane.component, rightPane.component)

  private var activePane = init(leftPane) { _ setActive true }
  private def otherPane = if (activePane == leftPane) rightPane else leftPane
  val sceneFocusChangeListener = changeListener { node: Node =>
    if (nodesUp(node) contains otherPane.component) {
      activePane setActive false
      otherPane setActive true
      activePane = otherPane
    }
  }

  val component: Parent = init(new BorderPane()) { pane =>
    pane setCenter splitPane
    pane setBottom init(new HBox()) { bottom =>
      bottom.getChildren add init (new Button(conf getString "button.rename.label")) { renameButton =>
        renameButton setOnAction handle(activePane renameSingleSelection())
      }
      bottom.getChildren add init (new Button(conf getString "button.copy.label")) { copyButton =>
      }
      bottom.getChildren add init (new Button(conf getString "button.move.label")) { moveButton =>
      }
      bottom.getChildren forEach new Consumer[Node] {
        override def accept(node: Node): Unit = node match { case button: Button =>
          HBox setHgrow (button, Priority.ALWAYS)
          button setPrefWidth 40
          button setMaxWidth Double.MaxValue
        }
      }
    }
  }
}
