// Copyright (c) 2013 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

trait CmdApp extends App {
  val usageHeader: String
  val paramData: Seq[((String, String), String)]
  
  lazy val usage: String = try {
    val lines = (usageHeader + "Parameters:") +: paramData.map{ case ((k, v), msg) => msg.format(k, v) }
    lines.mkString("\n")
  } catch { case e: Throwable => "Oops ... error while building usage string!" }
    
  def main(args: Array[String])(code: Map[String, String] => Unit) = {
    try {
      val defaults = paramData.map(_._1).toMap
      val opts = defaults ++ Args.toMap(args)
      require(opts.keySet == defaults.keySet, "Unexpected parameter(s): %s" format (opts.keySet -- defaults.keySet).mkString(" / "))
      code(opts)
    } catch {
      case e: Throwable =>
        println("%s\n\n%s\n%s" format (usage, e, e.getStackTraceString))
    }
  }
}