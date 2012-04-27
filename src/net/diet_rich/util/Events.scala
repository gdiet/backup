package net.diet_rich.util

class EventSource[T] extends Events[T] {
  // EVENTUALLY, derive an EventSource that emits in a separate thread
  def emit(event: T) : T = { observers foreach (_(event)) ; event }
}

class Events[T] {
  protected val observers = new scala.collection.mutable.Queue[T => Unit]()
  def subscribe(observer: T => Unit): Unit = observers += observer
}
