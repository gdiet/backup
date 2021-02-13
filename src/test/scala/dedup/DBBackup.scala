package dedup

object DBBackup extends App {
  sys.props.update("LOG_BASE", "./")
  Main.main(Array("dbbackup"))
}
