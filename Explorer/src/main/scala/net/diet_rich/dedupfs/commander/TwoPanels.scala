package net.diet_rich.dedupfs.commander

import javafx.scene.Parent
import javafx.scene.control.SplitPane

import net.diet_rich.common._

class TwoPanels(initialUrlLeft: String, initialUrlRight: String) {
  val component: Parent = init(new SplitPane()) { split =>
    split.getItems.add(new FilesPane(initialUrlLeft).component)
    split.getItems.add(new FilesPane(initialUrlRight).component)
  }
}
