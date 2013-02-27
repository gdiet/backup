// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import javax.swing._
import java.awt.event._
import java.awt.BorderLayout
import java.awt.Color
import javax.swing.text.DefaultCaret
import java.awt.Font

class SwingConsole private () extends Console { import SwingConsole._

  private val frame = new JFrame("app")
  private val textArea = new JTextArea
  private val textCaret = new DefaultCaret
  private val progressField = new JTextField
  private val inputField = new JTextField
  private val scrollPane = new JScrollPane
  private var text = ""
  private var lastProgress = System.currentTimeMillis
  private var timeInterval = 29500

  progressField.setEditable(false)
  textArea.setEditable(false)
  textArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13))
  textArea.setCaret(textCaret)
  // auto-scrolls to the bottom
  textCaret.setUpdatePolicy(DefaultCaret.ALWAYS_UPDATE)
  scrollPane.setViewportView(textArea)
  inputField.getCaret().setVisible(false);
  inputField.setEditable(false)
  
  frame.getContentPane().setLayout(new BorderLayout)
  frame.add(progressField, BorderLayout.NORTH)
  frame.add(scrollPane, BorderLayout.CENTER)
  frame.add(inputField, BorderLayout.SOUTH)

  frame.addWindowListener(new WindowAdapter {
    // FIXME hook for possibly closing the window
    override def windowClosing(e: WindowEvent): Unit = System.err.println("windowClosing")
  })
  
  frame.setSize(500, 400)
  frame.setLocationRelativeTo(null)
  frame.setVisible(true)
  frame.setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE)

  def close = runAndWait {
    textArea.setBackground(new Color(230,230,230))
    frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE)
  }

  def printProgress(string: String) = runLater {
    progressField.setText(string)
    if (System.currentTimeMillis - timeInterval > lastProgress) {
      timeInterval = timeInterval + 10000
      lastProgress = System.currentTimeMillis
      System.out.println("-> " + string)
      text = text + string + "\n"
      textArea.setText(text)
    }
  }
  
  def println(string: String) = runLater {
    System.out.println("-> " + string)
    text = text + string + "\n"
    textArea.setText(text)
  }

  def readln(string: String): String = {
    if (!string.isEmpty()) {
      println(string)
      System.out.println("Input only in the GUI console.")
    }
    val text = scala.concurrent.Promise[String]
    val background = inputField.getBackground()
    runAndWait {
      inputField.getCaret().setVisible(true);
      inputField.setBackground(new Color(200,255,230))
      inputField.setEditable(true)
      inputField.setText("")
      inputField.addActionListener(new ActionListener {
        override def actionPerformed(evt: ActionEvent) = {
          inputField.removeActionListener(this)
          text.complete(scala.util.Try(inputField.getText()))
        }
      })
    }
    val result = scala.concurrent.Await.result(text.future, scala.concurrent.duration.Duration.Inf)
    runAndWait {
      inputField.getCaret().setVisible(false);
      inputField.setBackground(background)
      inputField.setEditable(false)
    }
    result
  }
  
}

object SwingConsole {
  def runAndWait(task: => Unit) =
    SwingUtilities.invokeAndWait(new Runnable { override def run = task })
  def runLater(task: => Unit) =
    SwingUtilities.invokeLater(new Runnable { override def run = task })
  def create: SwingConsole = {
    val console = scala.concurrent.Promise[SwingConsole]
    runLater { console.complete(scala.util.Try(new SwingConsole)) }
    scala.concurrent.Await.result(console.future, scala.concurrent.duration.Duration.Inf)
  }
}

trait Console {
  def println(string: String): Unit
  def readln(string: String = ""): String
  def printProgress(string: String): Unit
  def close: Unit
}

object Console extends Console {
  def println(string: String): Unit = System.out.println(string)
  def readln(string: String): String = readLine(string)
  def printProgress(string: String): Unit = System.out.println(string)
  def close: Unit = Unit
}
