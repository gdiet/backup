package dedup

import java.awt.event.ActionEvent
import java.awt._

import javax.swing.{JOptionPane, SwingUtilities}

object TrayApp extends App {
  require(SystemTray.isSupported, "System tray not supported.")
  val icon: Image = new javax.swing.ImageIcon(getClass.getResource("/trayIcon.png"), "tray icon").getImage
  val freeSpaceItem = new MenuItem("Free: ??? MB")
  val stopItem = new MenuItem("Stop Dedup File System")
  val popup = new PopupMenu()
  popup.add(freeSpaceItem)
  popup.addSeparator()
  popup.add(stopItem)
  val trayIcon = new TrayIcon(icon, "tray icon")
  trayIcon.setImageAutoSize(true)
  trayIcon.setPopupMenu(popup)
  SystemTray.getSystemTray.add(trayIcon)

  stopItem.addActionListener{(e: ActionEvent) =>
    val reply = JOptionPane.showConfirmDialog(null, "Stop Dedup File System?", "Dedup File System", JOptionPane.YES_NO_OPTION)
    if (reply == 0) System.exit(0)
  }

  scala.concurrent.ExecutionContext.global.execute {() =>
    while(true) {
      import Runtime.{getRuntime => rt}
      val freeSpace = rt.maxMemory - rt.totalMemory + rt.freeMemory
      SwingUtilities.invokeLater(() => freeSpaceItem.setLabel(s"Free: ${freeSpace/1000000} MB"))
      Thread.sleep(5000)
    }
  }

  Server.main(args)
}
