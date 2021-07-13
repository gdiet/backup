package dedup

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.filter.ThresholdFilter
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import org.slf4j.{Logger, LoggerFactory}

import java.awt.event.{WindowAdapter, WindowEvent}
import java.awt.{Dimension, Font}
import java.text.SimpleDateFormat
import java.util.Date
import javax.swing._

class ServerGui(settings: server.Settings):
  private var lines = Vector[String]()
  private val dateFormat = SimpleDateFormat("HH:mm.ss")
  private val appender = new AppenderBase[ILoggingEvent] {
    override def append(e: ILoggingEvent): Unit =
      val exception = Option(e.getThrowableProxy).map(t => s": ${t.getClassName} - ${t.getMessage}").getOrElse("")
      val timeString = dateFormat.format(Date(e.getTimeStamp))
      val line = s"$timeString ${e.getLevel} ${e.getFormattedMessage}$exception"
      SwingUtilities.invokeLater { () =>
        lines = (lines :+ line).takeRight(15)
        textArea.setText(lines.mkString("\n"))
        textArea.setCaretPosition(0) // scroll left
      }
  }
  appender.addFilter(ThresholdFilter().tap(_.setLevel("INFO")).tap(_.start()))
  private val loggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
  appender.setContext(loggerContext)
  appender.start()
  private val rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME)
  rootLogger.addAppender(appender)

  private val pane = JPanel()
  pane.setLayout(BoxLayout(pane, BoxLayout.Y_AXIS))

  if !settings.readonly then
    val copyWhenMoveCheckbox = JCheckBox("copy when moving", settings.copyWhenMoving.get())
    copyWhenMoveCheckbox.addActionListener(_ => settings.copyWhenMoving.set(copyWhenMoveCheckbox.isSelected))
    pane.add(copyWhenMoveCheckbox)

  private val textArea = JTextArea()
  textArea.setFont(Font(Font.MONOSPACED, Font.PLAIN, 14))
  textArea.setEditable(false)
  private val scrollPane = JScrollPane(textArea)

  pane.add(scrollPane)
  private val frame = JFrame("Dedup file system")
  frame.getContentPane.add(pane)
  frame.setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE)
  frame.addWindowListener(new WindowAdapter {
    override def windowClosing(e: WindowEvent): Unit =
      val reply = JOptionPane.showConfirmDialog(frame, "Stop Dedup File System?", "Dedup File System", JOptionPane.YES_NO_OPTION)
      if reply == 0 then sys.exit(0)
  })
  private val icon = javax.swing.ImageIcon(getClass.getResource("/trayIcon.png"), "tray icon").getImage
  frame.setIconImage(icon)
  frame.setPreferredSize(Dimension(650, 356))
  frame.pack()
  frame.setLocationRelativeTo(null)
  frame.setVisible(true)
