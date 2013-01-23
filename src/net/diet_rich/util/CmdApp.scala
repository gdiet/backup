// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

trait CmdApp {
  val usageHeader: String
  val paramData: Seq[((String, String), String)]
  
  lazy val usage: String = try {
    val lines = (usageHeader + "Parameters:") +: paramData.map{ case ((k, v), msg) => msg.format(k, v) }
    lines.mkString("\n")
  } catch { case e: Throwable => "Oops ... error while building usage string!" }
    
  def run(args: Array[String])(code: Map[String, String] => Unit): Boolean = {
    try {
      val defaults = paramData.map(_._1).toMap
      val opts = defaults ++ Args.toMap(args)
      require(opts.keySet == defaults.keySet, s"Unexpected parameter(s): ${(opts.keySet -- defaults.keySet).mkString(" / ")}")
      code(opts)
      true
    } catch {
      case e: Throwable =>
        println(s"$usage\n\n$e\n${e.getStackTraceString}")
        if (e.getCause() != null) println(s"caused by ${e.getCause().getStackTraceString}")
        false
    }
  }
}