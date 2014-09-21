// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import java.io.File

import net.diet_rich.dedup.core.values.Size
import net.diet_rich.dedup.util.ConsoleApp

object CreateRepositoryApp extends ConsoleApp {
  checkUsage("parameters: <repository path> [hashAlgorithm:(MD5)] [dataBlockSize:(67108864)]")
  Repository.create(
    new File(repositoryPath),
    option("hashAlgorithm:", "MD5"),
    Size(option("dataBlockSize:", "67108864").toLong)
  )
}
