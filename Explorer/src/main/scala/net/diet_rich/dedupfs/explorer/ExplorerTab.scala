package net.diet_rich.dedupfs.explorer

import javafx.beans.property.{ObjectProperty, StringProperty}
import javafx.event.{ActionEvent, EventHandler}
import javafx.scene.Parent
import javafx.scene.control.{TextField, Button}
import javafx.scene.image.ImageView
import javafx.scene.layout.BorderPane

import net.diet_rich.common.init

class ExplorerTab {
  private val upButton = new Button()
  val up: ObjectProperty[EventHandler[ActionEvent]] = upButton.onActionProperty()

  private val pathField = new TextField()
  val path: StringProperty = pathField.textProperty()

  private val reloadButton = new Button()
  val reload: ObjectProperty[EventHandler[ActionEvent]] = reloadButton.onActionProperty()

  val component: Parent = init(new BorderPane()) { mainPane =>
    mainPane setTop {
      init(new BorderPane()) { topPane =>
        topPane setLeft {
          init(upButton) { up =>
            up setGraphic new ImageView(imageUp).fit(17, 17)
          }
        }
        topPane setCenter pathField
        topPane setRight {
          init(reloadButton) { reload =>
            reload setGraphic new ImageView(imageReload).fit(17, 17)
          }
        }
      }
    }
  }
}
