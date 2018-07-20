package net.diet_rich.scalafs

import net.diet_rich.util.ClassLogging

object StaticFS extends FileSystem with ClassLogging {
  override def getNode(path: String): Option[Node] = {
    log.info(s"getNode($path)")
    path match {
      case "/" => Some(Root)
      case "/hello" => Some(Hello)
      case _ => None
    }
  }
}

case object Root extends Dir {
  override def list: Seq[Node] = Seq(Hello)
  override def name: String = ""
}

case object Hello extends Dir {
  override def list: Seq[Node] = Seq()
  override def name: String = "hello"
}
