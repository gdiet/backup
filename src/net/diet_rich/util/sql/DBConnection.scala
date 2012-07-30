// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.sql

import java.sql.{Connection,DriverManager}
import net.diet_rich.util.ToString
import scala.sys.ShutdownHookThread

object DBConnection {
  
  def connectWithShutdownHook(jdbcDriverClassName: String, jdbcURL: String, jdbcUser: String, jdbcPassword: String): Connection = {
    Class forName jdbcDriverClassName
    val connection = DriverManager getConnection(jdbcURL, jdbcUser, jdbcPassword)
    connection setAutoCommit true
    // Without explicit shutdown, the last changes may not be written to file!
    ShutdownHookThread {
      // COMPACT: Good for HSQLDB. H2 at least does not mind - possibly also good.
      connection.createStatement execute "SHUTDOWN COMPACT;"
    }
    connection
  }

  // With the shutdown hook thread, we need ";DB_CLOSE_ON_EXIT=FALSE" in the JDBC
  // connect string to avoid an error message on shutdown at the second shutdown attempt.
  // h2 memory: jdbc:h2:mem:testdb;DB_CLOSE_ON_EXIT=FALSE
  // h2 file  : jdbc:h2:temp/testdb;DB_CLOSE_ON_EXIT=FALSE
  // h2 client: jdbc:h2:tcp://localhost/temp/testdb

  // HSQLDB memory: jdbc:hsqldb:mem:testdb
  // HSQLDB file  : jdbc:hsqldb:file:temp/testdb
  // HSQLDB client: jdbc:hsqldb:hsql://localhost/testdb

  // log4jdbc (http://code.google.com/p/log4jdbc) used for JDBC logging.
  // jdbcDriverClassName = "net.sf.log4jdbc.DriverSpy" auto-loads hsqldb and h2 driver if necessary.
  // To enable logging, add a "jdbc:log4" prefix to the JDBC URL.
  
  // As alternative to log4jdbc, jdbcdslog (http://code.google.com/p/jdbcdslog) could be used.
   
  val H2DRIVER = "org.h2.Driver"
  val HSQLDBDRIVER = "org.hsqldb.jdbc.JDBCDriver"
  
  private def connectToDB(db: ToString, logged: Boolean, urlTemplate: String, driver: String) : Connection = connectWithShutdownHook (
    jdbcDriverClassName = if (logged) "net.sf.log4jdbc.DriverSpy" else driver,
    jdbcURL = urlTemplate.format(if(logged) ":log4jdbc" else "", db),
    jdbcUser = "SA",
    jdbcPassword = ""
  )
  
  def hsqlMemoryDB(name: String = "memdb", logged: Boolean = false) : Connection =
    connectToDB(name, logged, "jdbc%s:hsqldb:mem:%s", HSQLDBDRIVER)
  
  def hsqlFileDB(dbdir: ToString, logged: Boolean = false) : Connection =
    connectToDB(dbdir, logged, "jdbc%s:hsqldb:file:%s", HSQLDBDRIVER)

  def h2MemoryDB(name: String = "memdb", logged: Boolean = false) : Connection =
    connectToDB(name, logged, "jdbc%s:h2:mem:%s;DB_CLOSE_ON_EXIT=FALSE", H2DRIVER)

  def h2FileDB(dbdir: ToString, logged: Boolean = false) : Connection =
    connectToDB(dbdir, logged, "jdbc%s:h2:%s;DB_CLOSE_ON_EXIT=FALSE", H2DRIVER)

}
