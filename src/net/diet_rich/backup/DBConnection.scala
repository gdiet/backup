// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import java.sql.DriverManager
import sys.ShutdownHookThread

class DBConnection(val settings: FSSettings) {

//  HSQLDB without logging:
//  Class forName "org.hsqldb.jdbc.JDBCDriver"
//  val connection = DriverManager getConnection("jdbc:hsqldb:file:temp/testdb", "SA", "")

//  HSQLDB with logging using log4jdbc (http://code.google.com/p/log4jdbc):
//  Class forName "net.sf.log4jdbc.DriverSpy" // auto-loads "org.hsqldb.jdbc.JDBCDriver"
//  val connection = DriverManager getConnection("jdbc:log4jdbc:hsqldb:file:temp/testdb", "SA", "")

//  Note: As alternative to log4jdbc, jdbcdslog (http://code.google.com/p/jdbcdslog) could be used.
  
  // TODO make database driver and URL configurable
  Class forName "net.sf.log4jdbc.DriverSpy" // auto-loads "org.hsqldb.jdbc.JDBCDriver"
  val connection = DriverManager getConnection("jdbc:log4jdbc:hsqldb:file:temp/testdb", "SA", "")
  connection setAutoCommit true

  // Note: Without explicit shutdown, the last changes may not be written to file!
  ShutdownHookThread {
    // HSQLDB explicit database shutdown:
    // A special form of closing the database is via the SHUTDOWN COMPACT command. 
    // This command rewrites the .data file that contains the information stored 
    // in CACHED tables and compacts it to its minimum size. This command should 
    // be issued periodically, especially when lots of inserts, updates or deletes 
    // have been performed on the cached tables. Changes to the structure of the 
    // database, such as dropping or modifying populated CACHED tables or indexes 
    // also create large amounts of unused file space that can be reclaimed using 
    // this command.
    connection.createStatement execute "SHUTDOWN COMPACT;"
  }
  
}