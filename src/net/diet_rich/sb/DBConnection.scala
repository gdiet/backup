// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import scala.sys.ShutdownHookThread

object DBConnection {
  def connect(jdbcDriverClassName: String, jdbcURL: String, jdbcUser: String, jdbcPassword: String) : Connection = {
    Class forName jdbcDriverClassName
    val connection = DriverManager getConnection(jdbcURL, jdbcUser, jdbcPassword)
    connection setAutoCommit true
  
    // EVENTUALLY add close() method to close connection without shutdown
    
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
    connection
  }
  
  // HSQLDB without logging: "org.hsqldb.jdbc.JDBCDriver"
  
  // log4jdbc (http://code.google.com/p/log4jdbc) used for JDBC logging.
  // Enable logging: jdbcDriverClassName = "net.sf.log4jdbc.DriverSpy" auto-loads hsqldb driver
  // Enable logging: Add a "jdbc:log4" prefix to the jdbcURL
  
  // Note: As alternative to log4jdbc, jdbcdslog
  // (http://code.google.com/p/jdbcdslog) could be used.
  
  def hsqlMemoryDB(name: String = "memdb") : Connection = connect(
    jdbcDriverClassName = "net.sf.log4jdbc.DriverSpy",
    jdbcURL = "jdbc:log4jdbc:hsqldb:mem:" + name,
    jdbcUser = "SA",
    jdbcPassword = ""
  )
  
  def h2MemoryDB(name: String = "memdb") : Connection = connect (
    jdbcDriverClassName = "net.sf.log4jdbc.DriverSpy",
    jdbcURL = "jdbc:log4jdbc:h2:mem:" + name,
    jdbcUser = "SA",
    jdbcPassword = ""
  )

  def hsqlFileDB(dbdir: File) : Connection = connect (
    jdbcDriverClassName = "net.sf.log4jdbc.DriverSpy",
    jdbcURL = "jdbc:log4jdbc:hsqldb:file:" + dbdir,
    jdbcUser = "SA",
    jdbcPassword = ""
  )

  def h2FileDB(dbdir: File) : Connection = connect (
    // Added ";DB_CLOSE_ON_EXIT=FALSE" to the db URL to disable automatic closing at VM shutdown.
    // Needed to execute SHUTDOWN COMPACT in the ShutdownHookThread (see DBConnection).
    jdbcDriverClassName = "net.sf.log4jdbc.DriverSpy",
    jdbcURL = "jdbc:h2:" + dbdir + ";DB_CLOSE_ON_EXIT=FALSE",
    jdbcUser = "SA",
    jdbcPassword = ""
  )

  // h2 driver: org.h2.Driver
  // h2 memory: jdbc:h2:mem:test;DB_CLOSE_ON_EXIT=FALSE
  // h2 file:   jdbc:h2:temp/testdb;DB_CLOSE_ON_EXIT=FALSE
  // h2 client: jdbc:h2:tcp://localhost/temp/testdb

}
