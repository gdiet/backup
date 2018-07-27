package net.diet_rich.util

trait ClassLogging {
  def log: Log.type = Log
}

object Log {
  def debug(message: String): Unit = info(message)
  def info(message: String): Unit = println(message)
}
