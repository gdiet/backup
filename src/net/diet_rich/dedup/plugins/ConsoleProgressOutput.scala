// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.plugins

/** message gets formatted with fileCount / dirCount / timeSeconds. */
class ConsoleProgressOutput(message: String, startAfter: Long, interval: Long) {
  
  private val startTime = System.currentTimeMillis()
  private val timer = new java.util.Timer("Progress Monitor")
  private val fileCount = new java.util.concurrent.atomic.AtomicLong(0)
  private val dirCount = new java.util.concurrent.atomic.AtomicLong(0)
  
  private def timeSeconds: Long = (System.currentTimeMillis() - startTime)/1000
  def incFiles: Unit = fileCount.incrementAndGet
  def incDirs: Unit = dirCount.incrementAndGet
  def cancel: Unit = timer.cancel
  
  timer.schedule(new java.util.TimerTask {def run = {
    println(message format(fileCount get, dirCount get, timeSeconds))
  }}, startAfter, interval)
}