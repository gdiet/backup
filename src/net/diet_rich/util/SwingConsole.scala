// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import javax.swing._
import java.awt.event._
import java.awt.BorderLayout
import java.awt.Color
import javax.swing.text.DefaultCaret

class SwingConsole extends Console {

  private val frame = new JFrame("app")
  private val textArea = new JTextArea
  private val textCaret = new DefaultCaret
  private val progressField = new JTextField
  private val inputField = new JTextField
  private val scrollPane = new JScrollPane
  private var text = ""

  progressField.setEditable(false)
  textArea.setEditable(false)
  textArea.setCaret(textCaret)
  textCaret.setUpdatePolicy(DefaultCaret.ALWAYS_UPDATE)
  scrollPane.setViewportView(textArea)
  inputField.setEditable(false)
  
  frame.getContentPane().setLayout(new BorderLayout)
  frame.add(progressField, BorderLayout.NORTH)
  frame.add(scrollPane, BorderLayout.CENTER)
  frame.add(inputField, BorderLayout.SOUTH)
  
  frame.setSize(500, 400)
  frame.setLocationRelativeTo(null)
  frame.setVisible(true)
  frame.setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE)

  def close = {
    textArea.setBackground(new Color(230,230,230))
    frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE)
  }

  def printProgress(string: String) = {
    System.out.println("-> " + string)
    progressField.setText(string)
  }
  
  def println(string: String) = {
    System.out.println("-> " + string)
    text = text + string + "\n"
    textArea.setText(text)
  }

  def readln(string: String): String = {
    if (!string.isEmpty()) println(string)
    inputField.setText("")
    inputField.setEditable(true)
    val text = scala.concurrent.Promise[String]
    inputField.addActionListener(new ActionListener {
      override def actionPerformed(evt: ActionEvent) = {
        inputField.removeActionListener(this)
        text.complete(scala.util.Try(inputField.getText()))
      }
    })
    val result = scala.concurrent.Await.result(text.future, scala.concurrent.duration.Duration.Inf)
    inputField.setEditable(false)
    result
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
