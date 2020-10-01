package dedup

object Reclaim1 extends App {
  sys.props.update("LOG_BASE", "./")
  Server.main(Array("reclaimspace1"))
}
