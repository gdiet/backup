// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.util

import java.io.File

trait ConsoleApp extends App {
  protected def checkUsage(usage: String) = require(!(args isEmpty), usage)
  final lazy val repositoryPath :: options = args.toList
  final lazy val repositoryDirectory =
    init(new File(repositoryPath)) { repositoryDirectory =>
      require(repositoryDirectory.isDirectory, s"$repositoryDirectory is not a directory")
    }
  def optional(key: String): Option[String] = options find (_ startsWith key) map (_ substring key.length)
  def option(key: String, default: => String): String = optional(key) getOrElse default
  def option(key: String): String = option(key, throw new IllegalArgumentException(s"option $key is mandatory"))
}
