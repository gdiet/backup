// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import java.io.File

import net.diet_rich.dedup.core.values.Size

object CreateRepository extends App {
  // FIXME copy/paste, see also ServerApp
  require(!(args isEmpty), "parameters: <repository path> [hashAlgorithm:(MD5)] [dataBlockSize:(67108864)]")
  val repositoryPath :: options = args.toList
  val hashAlgorithm: String = options find (_ startsWith "hashAlgorithm:") map (_ substring 14) getOrElse "MD5"
  val dataBlockSize: Long = (options find (_ startsWith "dataBlockSize:") map (_ substring 14) getOrElse "67108864").toLong
  Repository.create(new File(repositoryPath), hashAlgorithm, Size(dataBlockSize))
}
