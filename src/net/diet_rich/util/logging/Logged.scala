// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.logging

/**
 * simple logging trait that can be used for console and file logging as well as 
 * to display a nice event log on a GUI. The key strings are intended to be keys 
 * for looking up localized error message formatters.
 * 
 * this trait can easily be extended to include e.g. output to SLF4J.
 */
trait Logged {
  import Logged._
  
  /** optional specific logging object to use. */
  def logListener : Option[Logger] = defaultLogListener
  
  /**
   * log an error, then throw it (= never returns normally).
   * internally, the throwable's message is taken as log key,
   * and the throwable itself is logged as last argument.
   */
  def throwError[T](throwable: java.lang.Throwable, args: Any*) : T = {
    error(throwable.getMessage, args :+ throwable :_*)
    throw throwable
  }
  
  def error(key: String, args: Any*) = optionAndFlag("ERROR", key, args:_*)(_.error(key, args:_*))
  def warning(key: String, args: Any*) = optionAndFlag("WARN", key, args:_*)(_.warning(key, args:_*))
  def info(key: String, args: Any*) = optionAndFlag("INFO", key, args:_*)(_.info(key, args:_*))
  def debug(key: String, args: Any*) = optionAndFlag("DEBUG", key, args:_*)(_.debug(key, args:_*))
  
  private def optionAndFlag(level: String, key: String, args: Any*)(loggerFunction: => Logger => Boolean) =
    // if no log listener is defined or the log listener does not consume logs, log to stdout
    if (logListener.forall(loggerFunction(_)))
      printf("%s: %s %s\n", level, key, args.mkString("[", ", ", "]"))
  
}

object Logged {
  
  var defaultLogListener : Option[Logger] = None
  
}