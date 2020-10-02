package dedup

object DBBackup extends App {
  sys.props.update("LOG_BASE", "./")
  Server.main(Array("dbbackup"))
}
