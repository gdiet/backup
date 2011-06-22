// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.logging

/**
 * logger trait to implement custom logger objects, e.g. for nice 
 * event logs on a GUI. Logging is introduced in the code by 
 * mixing in the Logged trait. The key strings are intended to be keys 
 * for looking up localized error message formatters.
 * 
 * the log methods should return false if the log event should 
 * not be processed further after the log method
 */
trait Logger {

  def error(key: String, args: Any*) : Boolean
  def warning(key: String, args: Any*) : Boolean
  def info(key: String, args: Any*) : Boolean
  def debug(key: String, args: Any*) : Boolean
  
}

object Logger {
  
  object NULLLOGGER extends Logger {
    override def error(key: String, args: Any*) = false
    override def warning(key: String, args: Any*) = false
    override def info(key: String, args: Any*) = false
    override def debug(key: String, args: Any*) = false
  }
  
}