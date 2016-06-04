package net.diet_rich.dedupfs.explorer.filesPane.filesTable

import javafx.beans.property.SimpleStringProperty
import javafx.beans.value.{WritableValue, ObservableValue}

trait NameContainer extends ObservableValue[String] with WritableValue[String]

object NameContainer {
  class VetoException(text: String) extends RuntimeException(text)

  def apply(initialValue: String, vetoer: String => Boolean = _ => true): NameContainer = new SimpleStringProperty(initialValue) with NameContainer {
    override def setValue(string: String): Unit =
      if (vetoer(string)) throw new VetoException(s"Vetoed changing $getValue to $string")
      else super.setValue(string)
  }
}
