// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup

import java.io.{FileOutputStream, File}
import java.nio.file.Files
import java.text.SimpleDateFormat
import java.util.Date
import java.util.zip.{ZipEntry, ZipOutputStream}

import net.diet_rich.dedup.util.init
import net.diet_rich.dedup.util.io.{RichFile, using}

package object core {
  val hashAlgorithmKey = "hash algorithm"

  def zipBackup(sourceFile: File, targetDir: File, namePattern: String, date: Date = new Date): Unit = {
    val dateString = new SimpleDateFormat("yy-MM-dd_HH-mm-ss") format date
    val targetFile = targetDir / (namePattern format dateString)
    using(new ZipOutputStream(new FileOutputStream(targetFile))) { zipOut =>
      zipOut setLevel 9 // TODO the nio zip file system should do this for us as well
      zipOut putNextEntry new ZipEntry(sourceFile getName)
      Files copy (sourceFile toPath(), zipOut)
      zipOut closeEntry()
    }
  }

  def backupDatabase(repositoryDirectory: File): Unit = {
    val databaseFile = repositoryDirectory / "database" / "dedup.h2.db"
    if (databaseFile exists()) {
      val targetDir = init(repositoryDirectory / "database" / "backups")(_ mkdir)
      zipBackup(databaseFile, targetDir, "dedup-db_%s.zip", new Date(databaseFile lastModified()))
    }
  }
}
