package dedup

object WriteServerGui extends App {
  sys.props.update("LOG_BASE", "./")
  Main.main(Array("write", "gui=true", "repo=/media/georg/WD/dedupfs", "mount=/home/georg/temp/mnt"))
}
