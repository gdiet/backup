package dedup

import java.awt.event.{WindowAdapter, WindowEvent}
import java.awt.{Dimension, Font}
import java.text.SimpleDateFormat
import java.util.Date

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import javax.swing._
import org.slf4j.{Logger, LoggerFactory}

object ServerGui extends App {
  private var lines = Vector[String]()
  private val dateFormat = new SimpleDateFormat("HH:mm.ss")
  private val appender = new AppenderBase[ILoggingEvent] {
    override def append(e: ILoggingEvent): Unit = {
      val exception = Option(e.getThrowableProxy).map(t => s": ${t.getClassName} - ${t.getMessage}").getOrElse("")
      val timeString = dateFormat.format(new Date(e.getTimeStamp))
      val line = s"$timeString ${e.getLevel} ${e.getFormattedMessage}$exception"
      SwingUtilities.invokeLater { () =>
        lines = (lines :+ line).takeRight(15)
        textArea.setText(lines.mkString("\n"))
        textArea.setCaretPosition(0) // scroll left
      }
    }
  }
  val loggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
  appender.setContext(loggerContext)
  appender.start()
  val rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME)
  rootLogger.addAppender(appender)

  val textArea = new JTextArea()
  textArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 14))
  val scrollPane = new JScrollPane(textArea)
  val frame = new JFrame("Dedup file system")
  frame.getContentPane.add(scrollPane)
  frame.setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE)
  frame.addWindowListener(new WindowAdapter {
    override def windowClosing(e: WindowEvent): Unit = {
      val reply = JOptionPane.showConfirmDialog(frame, "Stop Dedup File System?", "Dedup File System", JOptionPane.YES_NO_OPTION)
      if (reply == 0) System.exit(0)
    }
  })
  val icon = new javax.swing.ImageIcon(getClass.getResource("/trayIcon.png"), "tray icon").getImage
  frame.setIconImage(icon)
  frame.setPreferredSize(new Dimension(650, 350))
  frame.pack()
  frame.setLocationRelativeTo(null)
  frame.setVisible(true)

  try Server.main(args) catch { case t: Throwable =>
    val exceptionMessage = t.getMessage.grouped(80).take(10).mkString("\n")
    JOptionPane.showMessageDialog(frame, s"Dedup File System exited abnormally:\n$exceptionMessage", "Dedup File System", JOptionPane. ERROR_MESSAGE)
    System.exit(0)
  }
}