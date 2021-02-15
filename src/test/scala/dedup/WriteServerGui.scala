package dedup

object WriteServerGui extends App {
  sys.props.update("LOG_BASE", "./")
  Main.main(Array("write", "gui=true", "mount=/home/georg/temp/mnt"))
}
