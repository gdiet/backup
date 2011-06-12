// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.logging

/**
 * logger trait to implement custom logger objects, e.g. for nice 
 * event logs on a GUI. Logging is introduced in the code by 
 * mixing in the Logged trait. The key strings are intended to be keys 
 * for looking up localized error message formatters.
 */
trait Logger {

  /** true if the log event should not be processed further after this call */
  def consumeLogs : Boolean
  
  def error(key: String, args: Any*)
  def warning(key: String, args: Any*)
  def info(key: String, args: Any*)
  def debug(key: String, args: Any*)
  
}