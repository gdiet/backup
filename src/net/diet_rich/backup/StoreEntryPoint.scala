// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import com.weiglewilczek.slf4s.Logging
import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import net.diet_rich.util.io.using

class StoreEntryPoint (fs: DedupFileSystem) extends Logging {

  def storeDir(storeRoot: String, dir: File) : Boolean = {
    // here: all checks before storing starts
    if (!dir.isDirectory) {
      logger error "Source " + dir + " is not a directory"
      false
    } else fs.make(storeRoot) match {
      case None =>
        logger error "Can't create store root " + storeRoot
        false
      case Some(id) =>
        storeAll(id, dir)
        true
    }
  }

  private val storeSpawnCount : Semaphore = new Semaphore(16) // TODO make configurable
  private val executor: ExecutorService = Executors.newFixedThreadPool(16) // TODO make configurable

  private def submit(task: => Unit)(errorHandler: Throwable => Unit)(cleanup: => Unit) : Unit =
    executor.submit(new Runnable { override def run : Unit = 
      try {
        task;
      } catch {
        case e => errorHandler(e)
      } finally {
        cleanup
      }
    })
  
  private def storeAll(storeRoot: Long, dir: File) : Unit = {
    logger info "Starting to back up directory " + dir
    try {
      storeDir(storeRoot, dir)
    } catch {
      case e => logger error ("Problem while parsing source", e) // TODO handle grave error
    }
    executor shutdown()
    executor awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
    logger info "Backup process finished"
  }

  private def storeDir(parentID: Long, source: File) : Unit = {
    logger debug "Storing directory " + source
    val files = source.listFiles
    if (files == null) {
      logger warn "Unexpectedly not a directory: " + source
    } else {
      files foreach (file =>
        if (file isDirectory) {
          fs make (parentID, file getName) match {
            case Some(id) => storeDir (id, file)
            case None => logger warn "Could not create target directory for " + file
          }
        } else if (file isFile) {
          storeSpawnCount acquire()
          submit{
            storeFile(parentID, file)
          }{
            logger error ("Problem while storing file", _)
          }{
            storeSpawnCount release()
          }
        } else {
          logger warn "Neither file nor directory: " + file
        }
      )
    }
  }

  private def storeFile(parentID: Long, source: File) : Unit =
    using(FileDataAccess(source, fs.settings.printCalculator, fs.settings.hashProvider)) { input =>
      logger debug "Processing file " + source
      // FIXME move this logic to file system
      val dataId, info = if (fs contains input.timeSizePrint) {
        val info = input.timeSizePrintHash
        fs dataId info match {
          case Some(id) => (id, info)
          case None => fs store input
        }
      } else fs store input
      // FIXME continue
    }
  
}