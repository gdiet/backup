// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.util

trait ConsoleApp extends App {
  protected def checkUsage(usage: String) = require(!(args isEmpty), usage)
  final lazy val repositoryPath :: options = args.toList
  def option(key: String, default: String): String = options find (_ startsWith key) map (_ substring key.length) getOrElse default
}
