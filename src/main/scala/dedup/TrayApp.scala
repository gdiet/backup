package dedup

import java.awt._

import javax.swing.JOptionPane

object TrayApp extends App {
  require(SystemTray.isSupported, "System tray not supported.")

  val stopItem = new MenuItem("Stop Dedup File System")
  val popup = new PopupMenu().tap(_.add(stopItem))
  val icon: Image = new javax.swing.ImageIcon(getClass.getResource("/trayIcon.png"), "tray icon").getImage
  val trayIcon = new TrayIcon(icon, "tray icon")
  trayIcon.setImageAutoSize(true)
  trayIcon.setPopupMenu(popup)
  SystemTray.getSystemTray.add(trayIcon)

  stopItem.addActionListener { _ =>
    val reply = JOptionPane.showConfirmDialog(null, "Stop Dedup File System?", "Dedup File System", JOptionPane.YES_NO_OPTION)
    if (reply == 0) System.exit(0)
  }

  try Server.main(args) catch { case t: Throwable =>
    val exceptionMessage = t.getMessage.grouped(80).take(10).mkString("\n")
    JOptionPane.showMessageDialog(null, s"Dedup File System exited abnormally:\n$exceptionMessage", "Dedup File System", JOptionPane. ERROR_MESSAGE)
    System.exit(0)
  }
}
