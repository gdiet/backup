package dedup2

object RunServer extends App {
  sys.props.update("LOG_BASE", "./")
  Server.main(Array("mount=/home/georg/temp/mnt", "write"))
}
