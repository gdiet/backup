// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers
import net.diet_rich.dedup.restore.Restore

class BackupAndRestoreSpec extends FlatSpec with ShouldMatchers {
  
  val base = new java.io.File("temp/tests/BackupAndRestoreSpec")
  val source1 = new java.io.File("testdata/source1")
  val source2 = new java.io.File("testdata/source2")
  val repository = new java.io.File(base, "repo")
  val restoreTo = new java.io.File(base, "restore")
  def clearBase = TestUtilites.clearDirectory(base)
  def clearRepository = TestUtilites.clearRepository(repository)
  
  val backupParameters = Map(
    "-i" -> "n",
    "-g" -> "n",
    "-s" -> source1.toString,
    "-r" -> repository.toString,
    "-t" -> "/target"
  )

  val restoreParameters = Map(
    "-g" -> "n",
    "-s" -> "/target",
    "-r" -> repository.toString,
    "-t" -> restoreTo.toString
  )
  
  "After restore, the files" should "be identical to the files that have been backed up" in {
    clearBase
    clearRepository
    Backup.run(backupParameters)
    Restore.run(restoreParameters)
    TestUtilites.dirContentsShouldBeEqual(source1, restoreTo)
  }

  "After restore of a differential backup, the files" should "be identical to the original files" in {
    clearBase
    clearRepository
    Backup.run(backupParameters)
    val secondBackup = backupParameters ++ Map(
      "-s" -> source2.toString,
      "-t" -> "/target2",
      "-d" -> "/target"
    )
    Backup.run(secondBackup)
    val restoreSecond = restoreParameters ++ Map(
      "-s" -> "/target2"
    )
    Restore.run(restoreSecond)
    TestUtilites.dirContentsShouldBeEqual(source2, restoreTo)
  }
  
}