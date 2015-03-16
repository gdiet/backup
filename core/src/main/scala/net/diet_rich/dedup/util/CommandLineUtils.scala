package net.diet_rich.dedup.util

import java.io.File

case class CommandLineUtils(args: Array[String]) extends Logging {
  val command :: repoPath :: options = args.toList
  log info s"$command: $repoPath"
  def repositoryDir = new File(repoPath)

  def optional(key: String): Option[String] =
    init(options find (_ startsWith s"$key:") map (_ substring key.length + 1)) {
      case None => log info s"option $key is not set"
      case Some(value) => log info s"option $key:$value"
    }

  def required(key: String): String =
    optional(key) getOrElse (throw new IllegalArgumentException(s"option '$key' is mandatory"))

  def intOptional(key: String) =
    optional(key) map (_ toInt)

  def booleanOptional(key: String) =
    optional(key) map (_ toBoolean)
}
