// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.plugins

import net.diet_rich.util._

/** message gets formatted with fileCount / dirCount / timeSeconds. */
class ConsoleProgressOutput(con: Console, message: String) extends java.io.Closeable {
  
  @volatile private var startTime = System.currentTimeMillis()
  private val timer = new java.util.Timer("Progress Monitor")
  private val fileCount = new java.util.concurrent.atomic.AtomicLong(0)
  private val dirCount = new java.util.concurrent.atomic.AtomicLong(0)
  private val interval = con match {
    case c: SwingConsole => 1000
    case _				 => 30000
  }
  
  private def timeSeconds: Long = (System.currentTimeMillis() - startTime)/1000
  def incFiles: Unit = fileCount.incrementAndGet
  def incDirs: Unit = dirCount.incrementAndGet
  def close(): Unit = timer.cancel
  def start: Unit = {
    startTime = System.currentTimeMillis()
    timer.schedule(new java.util.TimerTask {def run = {
      con.printProgress(message format(fileCount get, dirCount get, timeSeconds))
    }}, interval, interval)
  }
}