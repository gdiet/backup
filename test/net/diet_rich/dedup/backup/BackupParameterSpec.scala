// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

class BackupParameterSpec extends FlatSpec with ShouldMatchers {
  
  val repository = new java.io.File("temp/tests/BackupParameterSpec")
  def clearRepository = TestUtilites.clearRepository(repository)
  
  val minimalBackupParameters = Map(
    "-i" -> "n",
    "-s" -> "testdata/source1",
    "-r" -> repository.toString,
    "-t" -> "/target"
  )
  
  "A backup" should "succeed with the minimal backup parameters" in {
    clearRepository
    Backup.run(minimalBackupParameters)
  }
  
  "A backup" should "succeed with an empty config file" in {
    clearRepository
    Backup.run(minimalBackupParameters + ("-c" -> "testdata/emptyFile"))
  }
  
  "A backup" should "not succeed with less than the minimal backup parameters" in {
    clearRepository
    (minimalBackupParameters - "-i").keys.foreach { key =>
      val thrown = evaluating{ Backup.run(minimalBackupParameters - key) } should produce[IllegalArgumentException];
      thrown.getMessage should include ("is mandatory")
    }
  }
}