package dedup

import java.awt.Font
import java.awt.event.{WindowAdapter, WindowEvent}

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import javax.swing.{JFrame, JTextArea, SwingUtilities}
import org.slf4j.{Logger, LoggerFactory}

object ServerGui extends App {
  var lines = Vector[String]()

  val appender = new AppenderBase[ILoggingEvent] {
    override def append(e: ILoggingEvent): Unit = {
      println(s"hallo $e")
      val line = s"${e.getLevel} ${e.getTimeStamp} ${e.getFormattedMessage}"
      lines = (lines :+ line).takeRight(15)
      val text = lines.mkString("\n")
      SwingUtilities.invokeLater { () => textArea.setText(text) }
    }
  }
  appender.setContext(LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext])
  LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger].addAppender(appender)
  val log = LoggerFactory.getLogger("dedup.ServerGui")

  val textArea = new JTextArea(15, 80)
  textArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 14))
  val frame = new JFrame("Dedup file system")
  frame.getContentPane.add(textArea)
  frame.addWindowListener(new WindowAdapter {
    override def windowClosing(e: WindowEvent): Unit = System.exit(0)
  })
  frame.pack()
  frame.setLocationRelativeTo(null)
  frame.setVisible(true)
  scala.concurrent.ExecutionContext.global.execute {() =>
    for (n <- 1 to 100) {
      log.info(s"$n")
      Thread.sleep(1000)
    }
  }
}
