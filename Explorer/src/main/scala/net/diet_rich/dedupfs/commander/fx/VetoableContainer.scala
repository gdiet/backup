package net.diet_rich.dedupfs.commander.fx

import javafx.beans.property.SimpleObjectProperty
import javafx.beans.value.{ObservableValue, WritableValue}

// TODO only allow one vetoer
trait VetoableContainer[T] extends ObservableValue[T] with WritableValue[T] {
  protected var vetoListeners: Set[T => Boolean] = Set()
  def addVetoListener(listener: T => Boolean) = vetoListeners = vetoListeners + listener
  def removeVetoListener(listener: T => Boolean) = vetoListeners = vetoListeners - listener
}

object VetoableContainer {
  def apply[T](initialValue: T): VetoableContainer[T] = new SimpleObjectProperty[T](initialValue) with VetoableContainer[T] {
    override def setValue(v: T): Unit =
      if (vetoListeners.exists(_ (v))) throw new VetoException(s"Vetoed changing $this to $v")
      else super.setValue(v)
  }
}

class VetoException(text: String) extends RuntimeException(text)
