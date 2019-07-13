package dedup

object Main extends App {
  val (rawOptions, commands) = args.partition(_.contains("="))
  require(commands.length < 2, "Only one command can be executed.")
  val options = rawOptions.map(_.split("=", 2).pipe(o => o(0) -> o(1))).toMap

  commands.headOption match {
    case Some("fs") => Server.run(options)
    case Some("init") => Init.run(options)
    case Some("store") => Store.run(options)
    case Some("backup") => BackupDB.run(options)
    case _ =>
      println("Available commands: init(repo), store(repo, source, target), backup(repo), fs(repo, mount).")
      println("Provide parameters like repo in the form key=value.")
  }
}
