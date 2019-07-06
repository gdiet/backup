package dedup

import scala.util.chaining._

object Main extends App {
  val (rawOptions, commands) = args.partition(_.contains("="))
  require(commands.length == 1, "Exactly one command is mandatory.")
  val options = rawOptions.map(_.split("=", 2).pipe(o => o(0) -> o(1))).toMap
  val command = commands.head

  command match {
    case "fs" => ReadOnlyFS.run(options)
    case "init" => Init.run(options)
  }
}
