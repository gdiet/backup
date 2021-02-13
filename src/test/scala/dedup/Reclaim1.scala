package dedup

object Reclaim1 extends App {
  sys.props.update("LOG_BASE", "./")
  Main.main(Array("reclaimspace1"))
}
