// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import java.sql.Connection
import java.util.concurrent.atomic.AtomicLong
import net.diet_rich.util.sql._
import net.diet_rich.util.vals._

object SqlDBUtil {
  def readAsAtomicLong(statement: String)(implicit connection: Connection): AtomicLong =
    new AtomicLong(execQuery(statement)(_ longOption 1).nextOnly.get)
  
  implicit class ValuesFromSqlResult(r: WrappedSQLResult) {
    def size(column: Int) = Size(r long column)
    def print(column: Int) = Print(r long column)
    def hash(column: Int) = Hash(r bytes column)
    def time(column: Int) = Time(r long column)
    def position(column: Int) = Position(r long column)
    def method(column: Int) = Method(r int column)
    def nodeType(column: Int) = NodeType(r int column)
    def dataEntry(column: Int) = DataEntryID(r long column)
    def dataEntryOption(column: Int) = DataEntryID(r longOption column)
    def treeEntry(column: Int) = TreeEntryID(r long column)
    def treeEntryOption(column: Int) = TreeEntryID(r longOption column)
  }
}
