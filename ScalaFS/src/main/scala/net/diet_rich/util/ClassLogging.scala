package net.diet_rich.util

trait ClassLogging {
  def log: Log.type = Log
}

object Log {
  def info(message: String): Unit = {
    println(s"** $message")
  }
}
