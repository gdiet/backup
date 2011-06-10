// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.logging

/**
 * this trait can easily be extended to include e.g. output to SLF4J.
 * the key strings are intended to be keys for looking up the
 * localized error message formatters
 */
trait Logged {
  
  def throwError(throwable: java.lang.Throwable, key: String, args: Any*) = { print("error", key, args:_*); throw throwable }
  def error(key: String, args: Any*) = print("error", key, args:_*)
  def warning(key: String, args: Any*) = print("warn", key, args:_*)
  def info(key: String, args: Any*) = print("info", key, args:_*)
  def debug(key: String, args: Any*) = print("debug", key, args:_*)
  
  private def print(level: String, key: String, args: Any*) = printf("%s: %s %s", level, key, args.mkString("[", ", ", "]"))
  
}